package main

import (
	"flag"
)

type config struct {
	port       string
	replicaOf  string
	dir        string
	dbfilename string
}

func ParseConfig() *config {
	config := &config{}

	flag.StringVar(&config.port, "port", "6379", "Port for TCP server to listen to")
	flag.StringVar(&config.replicaOf, "replicaof", "", "The address for Master instance")
	flag.StringVar(&config.dir, "dir", "", "Directory path for RDB database file")
	flag.StringVar(&config.dbfilename, "dbfilename", "", "RDB database file name")

	flag.Parse()

	return config
}

func (c *config) Get(key string) (string, bool) {
	switch key {
	case "dir":
		return c.dir, true
	case "dbfilename":
		return c.dbfilename, true
	case "port":
		return c.port, true
	case "replicaOf":
		return c.replicaOf, true
	default:
		return "", false
	}
}
