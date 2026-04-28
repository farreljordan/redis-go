package main

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
)

func main() {
	config := ParseConfig()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	address := "0.0.0.0:" + config.port
	logger.Info("starting server", slog.String("address", address))

	listener, err := net.Listen("tcp", address)
	if err != nil {
		logger.Error(
			"cannot start tcp server",
			slog.String("address", address),
			slog.Any("err", err),
		)
		os.Exit(-1)
	}

	server := NewServer(listener, logger, config)
	go func() {
		if err := server.Start(); err != nil {
			logger.Error("server error", slog.Any("err", err))
			os.Exit(1)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	if err := server.Stop(); err != nil {
		logger.Error("cannot stop server", slog.Any("err", err))
		os.Exit(1)
	}
}
