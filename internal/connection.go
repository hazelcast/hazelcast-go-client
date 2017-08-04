package internal


import (
	"net"
	. "github.com/hazelcast/go-client/internal/protocol"
	"strconv"
)

type Connection struct {
	pending chan ClientMessage
	socket net.Conn
	clientMessageBuilder ClientMessageBuilder
	active chan bool
	endpoint *Address
}

func NewConnection(address *Address) (*Connection, error) {
	connection := Connection{}
	socket,err := net.Dial("tcp",address.Host()+":"+strconv.Itoa(address.Port()))
	if err != nil {
		return nil,err
	}
	connection.socket = socket
	return &connection,nil
}

func (connection *Connection) Send(clientMessage *ClientMessage) error {
	remainingLen := len(clientMessage.Buffer)
	writeIndex := 0
	for remainingLen > 0 {
		writtenLen,err:=connection.socket.Write(clientMessage.Buffer[writeIndex:])
		if err != nil {
			return err
		}else {
			remainingLen -= writtenLen
			writeIndex += writtenLen
		}
	}
	return nil

}

func (connection *Connection) Close() {

}