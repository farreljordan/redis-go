package redis

import (
	"log"
	"net"
	"strings"
)

var (
	MNode MasterNode
	SNode SlaveNode
)

type MasterNode struct {
	ReplicationId    string
	Host             string
	Port             string
	SlaveConnections []net.Conn
}

type SlaveNode struct {
	MasterReplicationId string
	Host                string
	Port                string
	MasterHost          string
	MasterPort          string
	Capability          string
}

func NewNode(config *Config) {
	if config.Role == "master" {
		MNode = MasterNode{
			ReplicationId:    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			Host:             "0.0.0.0",
			Port:             config.Port,
			SlaveConnections: make([]net.Conn, 0),
		}
	} else if config.Role == "slave" {
		SNode = SlaveNode{
			Host:       "0.0.0.0",
			Port:       config.Port,
			MasterHost: config.MasterHost,
			MasterPort: config.MasterPort,
		}
		SNode.handshakeToMaster()
	}
}

func (s *SlaveNode) handshakeToMaster() {
	url := s.MasterHost + ":" + s.MasterPort
	conn, err := net.Dial("tcp", url)
	//defer conn.Close()
	buf := make([]byte, 1024)

	if err != nil {
		log.Panic("Failed to connect to master", err)
		return
	}
	conn.Write([]byte(PING))
	n, err := conn.Read(buf)
	if string(buf[:n]) != PONG {
		log.Panic("Not receiving PONG from master", err)
		return
	}

	conn.Write(parseRESPArrays("REPLCONF", "listening-port", s.Port))
	n, err = conn.Read(buf)
	if string(buf[:n]) != OK {
		log.Panic("Not receiving OK from master", err)
		return
	}

	conn.Write(parseRESPArrays("REPLCONF", "capa", "psync2"))
	n, err = conn.Read(buf)
	if string(buf[:n]) != OK {
		log.Panic("Not receiving OK from master", err)
		return
	}

	conn.Write(parseRESPArrays("PSYNC", "?", "-1"))
	n, err = conn.Read(buf)
	response := string(buf[:n])
	responseList := strings.Split(response, " ")
	s.MasterReplicationId = responseList[1]
}

func (m *MasterNode) PropagateMessage(message []byte) {
	//fmt.Println("Propagate Message:\r\n" + string(message))
	//buf := make([]byte, 1024)
	for _, conn := range m.SlaveConnections {
		log.Println("Propagating Message")
		conn.Write(message)
		//n, err := conn.Read(buf)
		//if err != nil {
		//	log.Panic("Error when propagate message\r\n", err)
		//	return
		//}
		//log.Println("Propagate Response : " + string(buf[:n]))
	}
}
