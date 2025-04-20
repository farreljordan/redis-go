package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
)

func main() {
	port := flag.String("port", "6379", "Port for TCP server to listen to")
	replicaOf := flag.String("replicaof", "", "The address for Master instance")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	address := "0.0.0.0:" + *port
	logger.Info("starting server", slog.String("address", address))

	listener, err := net.Listen("tcp", address)
	if err != nil {
		logger.Error(
			"cannot start tcp server",
			slog.String("address", address),
			slog.String("err", err.Error()),
		)
		os.Exit(-1)
	}

	server := NewServer(listener, logger, *replicaOf)
	go func() {
		if err := server.Start(); err != nil {
			logger.Error("server error", slog.String("err", err.Error()))
			os.Exit(1)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	if err := server.Stop(); err != nil {
		logger.Error("cannot stop server", slog.String("err", err.Error()))
		os.Exit(1)
	}
}
