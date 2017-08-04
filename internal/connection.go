package internal

import (
	"errors"
	"net"
	"strconv"

	"sync/atomic"

	. "github.com/hazelcast/go-client/internal/protocol"
)

type Connection struct {
	pending              chan *ClientMessage
	socket               net.Conn
	clientMessageBuilder ClientMessageBuilder
	closed               chan bool
	endpoint             *Address
	sendingError         chan int64
	status               int32
}

func NewConnection(address *Address, responseChannel chan *ClientMessage, sendingError chan int64) *Connection {
	connection := Connection{clientMessageBuilder: ClientMessageBuilder{responseChannel: responseChannel}, sendingError: sendingError}
	go func() {
		socket, err := net.Dial("tcp", address.Host()+":"+strconv.Itoa(address.Port()))
		if err != nil {
			close(connection.closed)
		} else {
			connection.socket = socket
		}
	}()
	go connection.process()
	return &connection
}
func (connection *Connection) IsConnected() bool {
	return connection.socket != nil && connection.socket.RemoteAddr() != nil
}

func (connection *Connection) process() {
	go func() {
		//Writer process
		for {
			select {
			case request := <-connection.pending:
				err := connection.write(request)
				if err != nil {
					connection.sendingError <- request.CorrelationId()
				}
			case <-connection.closed:
				return
			}
		}
	}()
	go func() {
		//reader process
	}()
}

func (connection *Connection) Send(clientMessage *ClientMessage) error {
	select {
	case connection.pending <- clientMessage:
		return nil
	case <-connection.closed:
		return errors.New("Connection Closed.")
	}
}

func (connection *Connection) write(clientMessage *ClientMessage) error {
	remainingLen := len(clientMessage.Buffer)
	writeIndex := 0
	for remainingLen > 0 {
		writtenLen, err := connection.socket.Write(clientMessage.Buffer[writeIndex:])
		if err != nil {
			return err
		} else {
			remainingLen -= writtenLen
			writeIndex += writtenLen
		}
	}
	return nil

}

func (connection *Connection) Close() {
	if !atomic.CompareAndSwapInt32(&connection.status, 0, 1) {
		return
	}
	close(connection.closed)


}
