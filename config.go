package redis

import (
	"os"
)

type Config struct {
	Port       string
	Role       string
	MasterHost string
	MasterPort string
}

func NewConfig(port string, replica string) *Config {
	var masterPort string
	if replica != "*" {
		masterPort = os.Args[len(os.Args)-1]
	}
	return &Config{
		Port:       port,
		Role:       getRole(replica),
		MasterHost: replica,
		MasterPort: masterPort,
	}
}

func getRole(masterHost string) string {
	if masterHost == "*" {
		return "master"
	}
	return "slave"
}
