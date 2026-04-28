package main

import "net"

type connection struct {
	net.Conn
	offSetAck   chan int64
	prevCommand string // TODO: might need to remove, this is for TU8 test case
}

// TODO: save offset for each connection
// wrong, slave offset will be the same as masterReplOffset
func NewConnection(conn net.Conn) *connection {
	return &connection{
		Conn:      conn,
		offSetAck: make(chan int64, 1),
	}
}
