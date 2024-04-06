package main

import (
	"flag"
	"fmt"
	"github.com/farreljordan/redis-go"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	port := flag.String("port", "6379", "Port for TCP server to listen to")
	replica := flag.String("replicaof", "*", "The address for Master instance")
	flag.Parse()

	err := redis.Listen(*port, *replica)
	if err != nil {
		os.Exit(1)
	}
}
