package main

import (
	"log"
	"strconv"
	"strings"
)

type Message struct {
	Command   string
	Key       string
	Value     string
	Arguments []string
	Size      int
}

func ParseMessage(bytes []byte) (*Message, error) {
	msgStr := string(bytes)
	log.Println("Received message:\r\n", msgStr)
	msgLst := strings.Split(msgStr, "\r\n")
	msgSize, err := strconv.Atoi(msgLst[0][1:])
	if err != nil {
		log.Printf("Error when parsing message length with error of %v\n", err)
		return nil, err
	}
	log.Printf("Message length: %v\n", msgSize)

	var (
		command   string
		key       string
		value     string
		arguments []string
	)
	for i, msg := range msgLst {
		if i == 2 {
			command = msg
		}
		if i == 4 {
			key = msg
		}
		if i == 6 {
			value = msg
		}
		if i > 7 && i%2 == 0 {
			arguments = append(arguments, msg)
		}
	}

	return &Message{
		Command:   command,
		Key:       key,
		Value:     value,
		Arguments: arguments,
		Size:      msgSize,
	}, nil
}
