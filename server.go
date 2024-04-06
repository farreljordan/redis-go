package redis

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

const (
	TEMPLATE_SIMPLE_STRINGS = "+%v\r\n"
	TEMPLATE_BULK_STRINGS   = "$%d\r\n%s\r\n"
	TEMPLATE_ARRAY_ITEM     = "$%d\r\n%s\r\n"
)
const (
	NULL = "$-1\r\n"
	OK   = "+OK\r\n"
	PING = "*1\r\n$4\r\nping\r\n"
	PONG = "+PONG\r\n"
)

func Listen(port string, replica string) error {
	config := NewConfig(port, replica)
	NewNode(config)

	l, err := net.Listen("tcp", "0.0.0.0:"+config.Port)
	if err != nil {
		fmt.Println("Failed to bind to port " + config.Port)
		return err
	}
	defer l.Close()
	fmt.Println("Server is listening on port " + config.Port)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error: ", err)
			continue
		}
		go handleClient(conn, config.Role)
	}
}

func handleClient(conn net.Conn, role string) {
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if errors.Is(err, io.EOF) {
			continue
		}
		if err != nil {
			log.Panic("Failed to read from the connection", err)
		}
		if n == 0 {
			continue
		}
		now := time.Now()

		message, err := ParseMessage(buf[:n])
		if err != nil {
			log.Panic("Failed to parse the message", err)
			return
		}

		switch strings.ToLower(message.Command) {
		case "ping":
			conn.Write([]byte(PONG))
		case "echo":
			conn.Write(parseRESPBulkStrings(message.Key))
		case "set":
			//go MNode.PropagateMessage([]byte(string(buf[:n])[1:]))
			go MNode.PropagateMessage(buf[:n])
			var ExpireDate time.Time
			if len(message.Arguments) > 0 && message.Arguments[0] == "px" {
				duration, err := time.ParseDuration(message.Arguments[1] + "ms")
				if err != nil {
					log.Panic("Failed to parse the duration", err)
				}
				ExpireDate = now.Add(duration)
			}
			db.Store(message.Key, Entry{Value: message.Value, ExpireDate: ExpireDate})
			log.Printf("Key: %v, Value: %v,ExpireDate: %v", message.Key, message.Value, ExpireDate)
			conn.Write([]byte(OK))
		case "get":
			entry, ok := db.Load(message.Key)
			if !ok {
				log.Printf("Entry with Key of %v doesn't exist", message.Key)
				conn.Write([]byte(NULL))
				continue
			}
			if !entry.ExpireDate.IsZero() && now.After(entry.ExpireDate) {
				log.Printf("Key %v already expired at %v\n", message.Key, entry.ExpireDate.String())
				conn.Write([]byte(NULL))
				continue
			}
			log.Printf("Key: %v, Value: %v,ExpireDate: %v", message.Key, entry.Value, entry.ExpireDate)
			conn.Write(parseRESPBulkStrings(entry.Value))
		case "info":
			conn.Write(parseRESPBulkStrings("role:"+role, "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", "master_repl_offset:0"))
		case "replconf":
			if message.Key == "listening-port" {
				MNode.SlaveConnections = append(MNode.SlaveConnections, conn)
			}
			if message.Key == "capa" {
				// message.Value
			}
			conn.Write([]byte(OK))
		case "psync":
			//if message.Key == "?" && message.Value == "-1" {
			conn.Write(parseRESPSimpleStrings(fmt.Sprintf("FULLRESYNC %s %d", MNode.ReplicationId, 0)))
			RDBContent, err := hex.DecodeString(EMPTY_DB_HEX)
			if err != nil {
				log.Panic("Failed to decode DB hex", err)
			}
			conn.Write([]byte(fmt.Sprintf("$%v\r\n%s", len(RDBContent), RDBContent)))
			//}
		default:
			log.Panic("Command not found")
			conn.Write([]byte(NULL))
		}
	}
}

func parseRESPSimpleStrings(response string) []byte {
	return []byte(fmt.Sprintf(TEMPLATE_SIMPLE_STRINGS, response))
}

func parseRESPArrays(responses ...string) []byte {
	data := ""
	for _, response := range responses {
		data += fmt.Sprintf(TEMPLATE_ARRAY_ITEM, len(response), response)
	}
	array := fmt.Sprintf("*%d\r\n%s", len(responses), data)
	log.Printf("Array: %s\n", array)
	return []byte(array)
}

func parseRESPBulkStrings(responses ...string) []byte {
	data := responses[0]
	for _, response := range responses[1:] {
		data += "\r\n" + response
	}
	bulkString := fmt.Sprintf(TEMPLATE_BULK_STRINGS, len(data), data)
	log.Printf("Response:\r\n%s", bulkString)
	return []byte(bulkString)
}
