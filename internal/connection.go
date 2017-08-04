package internal

import (
	"github.com/hazelcast/go-client"
	"net"
)

type Connection struct {
	pending chan hazelcast.ClientMessage
	socket net.Conn
	clientMessageBuilder ClientMessageBuilder
	active chan bool
}

func NewConnection(address hazelcast.Address) (*Connection, error) {

}

func (connection *Connection) Send(clientMessage *hazelcast.ClientMessage) error {


}

func (connection *Connection) Close() {

}