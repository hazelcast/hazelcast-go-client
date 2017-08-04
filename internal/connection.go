package internal


import (
	"net"
	. "github.com/hazelcast/go-client/core"
	. "github.com/hazelcast/go-client/internal/protocol"
)

type Connection struct {
	pending chan ClientMessage
	socket net.Conn
	clientMessageBuilder ClientMessageBuilder
	active chan bool
}

func NewConnection(address Address) (*Connection, error) {
	return nil, nil
}

func (connection *Connection) Send(clientMessage *ClientMessage) error {
	return nil
}

func (connection *Connection) Close() {

}