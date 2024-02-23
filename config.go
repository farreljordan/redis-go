package main

import (
	"flag"
	"os"
)

type Config struct {
	Port       string
	Role       string
	MasterHost string
	MasterPort string
}

var (
	port    = flag.String("port", "6379", "Port for TCP server to listen to")
	replica = flag.String("replicaof", "*", "The address for Master instance")
)

func NewConfig() *Config {
	flag.Parse()
	var masterPort string
	if *replica != "*" {
		masterPort = os.Args[len(os.Args)-1]
	}
	return &Config{
		Port:       *port,
		Role:       getRole(*replica),
		MasterHost: *replica,
		MasterPort: masterPort,
	}
}

func getRole(masterHost string) string {
	if masterHost == "*" {
		return "master"
	}
	return "slave"
}
