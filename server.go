package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

type Record struct {
	Value      string
	ExpireDate time.Time
}

var (
	records = make(map[string]Record)
)

const (
	TEMPLATE_BULK_STRING = "$%d\r\n%s\r\n"
	NULL                 = "$-1\r\n"
	OK                   = "+OK\r\n"
	PING                 = "*1\r\n$4\r\nping\r\n"
	PONG                 = "+PONG\r\n"
)

func main() {
	fmt.Println("Logs from your program will appear here!")
	config := NewConfig()

	if config.Role == "slave" {
		handshakeToMaster(config.MasterHost + ":" + config.MasterPort)
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+config.Port)
	if err != nil {
		fmt.Println("Failed to bind to port " + config.Port)
		os.Exit(1)
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

func handshakeToMaster(url string) {
	conn, err := net.Dial("tcp", url)
	if err != nil {
		return
	}
	conn.Write([]byte(PING))
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
			log.Printf("Failed to read from connection: %v\n", err)
		}
		if n == 0 {
			continue
		}
		now := time.Now()

		message, err := ParseMessage(buf[:n])
		if err != nil {
			log.Println("Error when parsing message")
			return
		}

		switch strings.ToLower(message.Command) {
		case "ping":
			conn.Write([]byte(PONG))
		case "echo":
			conn.Write([]byte(generateBulkString([]string{message.Key})))
		case "set":
			var ExpireDate time.Time
			if len(message.Arguments) > 0 && message.Arguments[0] == "px" {
				duration, err := time.ParseDuration(message.Arguments[1] + "ms")
				if err != nil {
					log.Println("Error parsing time")
				}
				ExpireDate = now.Add(duration)
			}
			records[message.Key] = Record{Value: message.Value, ExpireDate: ExpireDate}
			log.Printf("Key: %v, Value: %v,ExpireDate: %v", message.Key, message.Value, ExpireDate)
			conn.Write([]byte(OK))
		case "get":
			record, ok := records[message.Key]
			if !ok {
				log.Printf("Record with Key of %v doesn't exist", message.Key)
				conn.Write([]byte(NULL))
				continue
			}
			if !record.ExpireDate.IsZero() && now.After(record.ExpireDate) {
				log.Printf("Key %v already expired at %v\n", message.Key, record.ExpireDate.String())
				conn.Write([]byte(NULL))
				continue
			}
			log.Printf("Key: %v, Value: %v,ExpireDate: %v", message.Key, record.Value, record.ExpireDate)
			conn.Write([]byte(generateBulkString([]string{record.Value})))
		case "info":
			responses := []string{"role:" + role, "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", "master_repl_offset:0"}
			conn.Write([]byte(generateBulkString(responses)))
		default:
			log.Println("Command not found")
			conn.Write([]byte(NULL))
		}
	}
}

func generateBulkString(responses []string) string {
	data := responses[0]
	for _, response := range responses[1:] {
		data += "\r\n" + response
	}
	bulkString := fmt.Sprintf(TEMPLATE_BULK_STRING, len(data), data)
	log.Printf("Response:\r\n%s", bulkString)
	return bulkString
}
