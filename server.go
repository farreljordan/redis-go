package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type server struct {
	listener net.Listener
	logger   *slog.Logger

	database Database

	role      string
	masterUrl string
	id        string

	masterConn *connection
	slavesConn []*connection
	// TODO: remove this and replace with offset in each connection
	// TODO: or this offset is for slave only
	offset int64
	mu     sync.RWMutex
}

func NewServer(listener net.Listener, logger *slog.Logger, replicaOf string) *server {
	role := "master"
	var masterUrl string
	if replicaOf != "" {
		role = "slave"
		masterUrl = strings.Replace(replicaOf, " ", ":", 1)
	}

	return &server{
		listener:  listener,
		logger:    logger,
		database:  *NewDatabase(),
		role:      role,
		masterUrl: masterUrl,
		id:        generateId(),
		offset:    0,
	}
}

func generateId() string {
	bytes := make([]byte, 20) // 20 bytes = 40 hex characters
	_, err := rand.Read(bytes)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

// TODO: Review fmt.ErrorF and s.logger.Error
func (s *server) Start() error {
	if s.role == "slave" {
		s.logger.Info("connecting to master")
		err := s.connectToMaster()
		if err != nil {
			s.logger.Error(
				"failed to connect to master",
				slog.String("id", s.id),
				slog.String("masterUrl", s.masterUrl),
				slog.String("err", err.Error()),
			)
			return err
		}
		s.logger.Info("connected to master")
	}

	s.logger.Info("server started")
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.logger.Error(
				"failed to accept connection",
				slog.String("err", err.Error()),
			)
			return err
		}
		connection := NewConnection(conn)
		reader := NewReader(conn)
		writer := NewWriter(conn)
		go s.handleConn(connection, reader, writer)
	}
}

func (s *server) Stop() error {
	if err := s.listener.Close(); err != nil {
		s.logger.Error(
			"cannot stop listener",
			slog.String("err", err.Error()),
		)
		return err
	}

	return nil
}

func (s *server) handleConn(conn *connection, reader *Reader, writer *Writer) {
	defer conn.Close()
	s.logger.Info(
		"client connected",
		slog.String("id", s.id),
		slog.String("host", conn.RemoteAddr().String()),
	)

	for {
		value, err := reader.Read()
		if err != nil {
			s.logger.Error(
				"failed to parse message",
				slog.String("id", s.id),
				slog.String("host", conn.RemoteAddr().String()),
				slog.Any("err", err),
			)
			break
		}

		err = func(value Value) error {
			if value.typ != "array" {
				return fmt.Errorf("message type should be array")
			}
			if len(value.array) == 0 {
				return fmt.Errorf("array should not be empty")
			}
			return nil
		}(value)
		if err != nil {
			s.logger.Error(
				"failed to process request",
				slog.Any("command", value),
				slog.Any("err", err),
			)
			break
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		switch command {
		case "PING":
			err = s.handlePing(*writer)
		case "ECHO":
			err = writer.Write(Value{typ: "string", str: args[0].bulk})
		case "GET":
			err = s.handleGet(args, *writer)
		case "SET":
			err = s.handleSet(args, *writer)
			go s.propagateMessage(value)
		case "DEL":
			err = s.handleDel(args, *writer)
			go s.propagateMessage(value)
		case "INFO":
			err = s.handleInfo(args, *writer)
		case "REPLCONF":
			err = s.handleReplconf(args, *writer, *conn)
		case "PSYNC":
			err = s.handlePsync(args, *writer, *conn)
		case "WAIT":
			err = s.handleWait(args, *writer, *conn)
		}

		// Replicas should update their offset to account for all commands propagated from the master, including PING and REPLCONF itself.
		if (conn == s.masterConn) || (s.role == "master" && (command == "SET" || command == "DEL")) {
			s.mu.RLock()
			s.offset += int64(value.size)
			s.mu.RUnlock()
		}
		conn.prevCommand = command

		if err != nil {
			s.logger.Error(
				"error handling command",
				slog.Any("command", value),
				slog.Any("err", err),
			)
			break
		}
	}
}

func (s *server) handleConnMaster(conn *connection, reader *Reader, writer *Writer) {
	defer func() {
		for i, slaveConn := range s.slavesConn {
			if slaveConn == conn {
				s.slavesConn = append(s.slavesConn[:i], s.slavesConn[i+1:]...)
				break
			}
		}
	}()
	// connection will be closed in this function
	s.handleConn(conn, reader, writer)
}

func (s *server) handlePing(writer Writer) error {
	if s.role == "slave" {
		return nil
	}

	return writer.Write(Value{typ: "string", str: "PONG"})
}

func (s *server) handleGet(args []Value, writer Writer) error {
	key := args[0].bulk
	s.logger.Info("get command received", slog.String("key", key), slog.String("role", s.role))

	if entry, ok := s.database.Load(key); ok {
		return writer.Write(Value{typ: "bulk", bulk: entry.value})
	} else {
		return writer.Write(Value{typ: "null"})
	}
}

func (s *server) handleSet(args []Value, writer Writer) error {
	key := args[0].bulk
	value := args[1].bulk
	var px string
	s.logger.Debug("set command received", slog.String("key", key), slog.String("value", value), slog.String("role", s.role))

	if len(args) >= 4 && strings.ToUpper(args[2].bulk) == "PX" {
		px = args[3].bulk
	}
	if err := s.database.Store(key, value, px); err != nil {
		return err
	}

	if s.role == "slave" {
		return nil
	}
	return writer.Write(Value{typ: "string", str: "OK"})
}

func (s *server) handleDel(args []Value, writer Writer) error {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.bulk
	}
	deleted := s.database.Delete(keys)

	if s.role == "slave" {
		return nil
	}
	return writer.Write(Value{typ: "integer", num: int64(deleted)})
}

func (s *server) handleInfo(args []Value, writer Writer) error {
	_ = args[0].bulk
	return writer.Write(Value{
		typ:  "bulk",
		bulk: fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", s.role, s.id, s.offset), // TODO: Return the specified conn offset
	})
}

func (s *server) handleReplconf(args []Value, writer Writer, conn connection) error {
	arg1 := args[0].bulk
	s.logger.Debug("replconf command received", slog.String("arg1", arg1), slog.String("role", s.role))
	switch strings.ToUpper(arg1) {
	case "LISTENING-PORT":
		return writer.Write(Value{typ: "string", str: "OK"})
	case "CAPA":
		return writer.Write(Value{typ: "string", str: "OK"})
	case "GETACK":
		s.logger.Debug("getack command received", slog.String("role", s.role))
		return writer.Write(Value{
			typ: "array",
			array: []Value{
				{typ: "bulk", bulk: "REPLCONF"},
				{typ: "bulk", bulk: "ACK"},
				{typ: "bulk", bulk: fmt.Sprintf("%d", s.offset)},
			},
		})
	case "ACK":
		s.logger.Debug("ack command received", slog.Any("args", args), slog.String("role", s.role))
		offset, err := strconv.Atoi(args[1].bulk)
		if err != nil {
			return fmt.Errorf("failed to process %s as offset: %s", args[1].bulk, err)
		}
		conn.offSetAck <- int64(offset)
		s.logger.Debug(
			"offset sent to conn.offSetAck",
			slog.Int64("offset", int64(offset)),
			slog.Any("conn", conn),
			slog.Any("conn.offSetAck", conn.offSetAck),
		)
		return nil
	default:
		writer.Write(Value{typ: "null"})
		return fmt.Errorf("invalid replconf command %s", arg1)
	}

}

// TODO: Handle actual implementation
func (s *server) handlePsync(args []Value, writer Writer, conn connection) error {
	_ = args[0].bulk
	if err := writer.Write(Value{typ: "string", str: fmt.Sprintf("FULLRESYNC %s %d", s.id, s.offset)}); err != nil { // TODO: Return the specified conn offset
		return err
	}
	RDBContent, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		return err
	}
	if err := writer.Write(Value{typ: "bulkWithoutCRLF", bulk: string(RDBContent)}); err != nil {
		return err
	}

	s.slavesConn = append(s.slavesConn, &conn)

	return nil
}

func (s *server) handleWait(args []Value, writer Writer, conn connection) error {
	numReplicas, err := strconv.Atoi(args[0].bulk)
	if err != nil {
		return fmt.Errorf("failed to process %s as numReplicas: %s", args[0].bulk, err)
	}
	timeout, err := strconv.Atoi(args[1].bulk)
	if err != nil {
		return fmt.Errorf("failed to process %s as timeout: %s", args[1].bulk, err)
	}

	// TODO: might need to remove, this is for TU8 test case
	if (conn.prevCommand != "SET") && (conn.prevCommand != "DEL") {
		return writer.Write(Value{typ: "integer", num: int64(len(s.slavesConn))})
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
	defer cancel()

	ackChan := make(chan int, len(s.slavesConn)) // Buffered channel to track acknowledgments
	var wg sync.WaitGroup

	val := Value{
		typ: "array",
		array: []Value{
			{typ: "bulk", bulk: "REPLCONF"},
			{typ: "bulk", bulk: "GETACK"},
			{typ: "bulk", bulk: "*"},
		},
		size: 37, // TODO: calculate size based on the actual value
	}

	for _, conn := range s.slavesConn {
		wg.Add(1)
		go func(conn *connection, val Value) {
			defer wg.Done()
			writer := NewWriter(conn)

			for {
				err := writer.Write(val)
				if err != nil {
					s.logger.Error(
						"failed to send REPLCONF GETACK to slave",
						slog.String("slave", conn.RemoteAddr().String()),
						slog.Any("err", err),
					)
					return
				}

				// s.logger.Debug(
				// 	"waiting for ack from slave",
				// 	slog.Any("conn", conn),
				// 	slog.Any("conn.offSetAck", conn.offSetAck),
				// )
				select {
				case offsetAck := <-conn.offSetAck:
					s.logger.Debug(
						"ack received from slave",
						slog.Int64("offset", offsetAck),
						slog.Int64("expected", s.offset),
					)
					// TODO: check specified conn offset
					if s.offset == offsetAck {
						ackChan <- 1
						return
					}
				case <-ctx.Done():
					// Stop waiting if the context is canceled (timeout or enough replicas acknowledged)
					s.logger.Debug(
						"timeout or enough replicas acknowledged",
					)
					return
				}

				// Add a small delay to avoid spamming the slave with requests
				time.Sleep(100 * time.Millisecond)
			}
		}(conn, val)
	}

	// Wait for all goroutines to finish or for the required number of replicas to acknowledge
	go func() {
		wg.Wait()
		close(ackChan)
	}()

	ackReplicas := 0
	for {
		select {
		case <-ctx.Done():
			// TODO: Need to update this based on how many time master sent the command to replicas
			// Since master can send the command multiple times if the offsetAck is not equal to the offset
			s.mu.RLock()
			s.offset += int64(val.size)
			s.logger.Debug(
				"offset updated",
				slog.Int("size", val.size),
			)
			s.mu.RUnlock()
			return writer.Write(Value{typ: "integer", num: int64(ackReplicas)})
		case ack := <-ackChan:
			ackReplicas += ack
			if ackReplicas >= numReplicas {
				// TODO: Need to update this based on how many time master sent the command to replicas
				// Since master can send the command multiple times if the offsetAck is not equal to the offset
				s.mu.RLock()
				s.offset += int64(val.size)
				s.logger.Debug(
					"offset updated",
					slog.Int("size", val.size),
				)
				s.mu.RUnlock()
				return writer.Write(Value{typ: "integer", num: int64(ackReplicas)})
			}
		}
	}
}

func (s *server) connectToMaster() error {
	conn, err := net.Dial("tcp", s.masterUrl)
	if err != nil {
		return err
	}

	reader := NewReader(conn)
	writer := NewWriter(conn)

	sendCommand := func(args ...string) error {
		values := make([]Value, len(args))
		for i, arg := range args {
			values[i] = Value{typ: "bulk", bulk: arg}
		}
		return writer.Write(Value{typ: "array", array: values})
	}

	sendAndExpect := func(expected string, args ...string) error {
		if err := sendCommand(args...); err != nil {
			return err
		}

		value, err := reader.Read()
		if err != nil {
			return err
		}

		if strings.ToUpper(value.str) != expected {
			return fmt.Errorf("invalid response %s, expected %s", value.str, expected)
		}

		return nil
	}

	if err := sendAndExpect("PONG", "PING"); err != nil {
		return err
	}

	_, port, err := net.SplitHostPort(s.listener.Addr().String())
	if err != nil {
		return err
	}

	if err := sendAndExpect("OK", "REPLCONF", "listening-port", port); err != nil {
		return err
	}

	if err := sendAndExpect("OK", "REPLCONF", "capa", "psync2"); err != nil {
		return err
	}

	if err := sendCommand("PSYNC", "?", "-1"); err != nil {
		return err
	}
	//TODO: Get repl_id
	res, err := reader.Read()
	if err != nil {
		return err
	}
	s.logger.Debug("received replId", slog.String("res", res.str))

	db, err := reader.ReadBulkWithoutCRLF()
	if err != nil {
		return err
	}
	s.logger.Debug("received db", slog.String("db", db.bulk))

	s.masterConn = NewConnection(conn)
	go s.handleConnMaster(s.masterConn, reader, writer)

	return nil
}

func (s *server) propagateMessage(value Value) {
	for _, conn := range s.slavesConn {
		writer := NewWriter(conn)
		s.logger.Debug("writing to slave", slog.String("slave", conn.RemoteAddr().String()))
		err := writer.Write(value)
		if err != nil {
			s.logger.Error(
				"failed to propagate message",
				slog.Any("err", err),
			)
		}
	}
}
