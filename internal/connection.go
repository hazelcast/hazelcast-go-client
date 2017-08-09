package internal

import (
	//	"errors"
	"net"
	"strconv"

	"sync/atomic"

	"fmt"
	. "github.com/hazelcast/go-client/internal/protocol"
)

type Connection struct {
	pending              chan *ClientMessage
	received             chan *ClientMessage
	socket               net.Conn
	clientMessageBuilder ClientMessageBuilder
	closed               chan bool
	endpoint             *Address
	sendingError         chan int64
	status               int32
	isOwnerConnection    bool
}

func NewConnection(address *Address, responseChannel chan *ClientMessage, sendingError chan int64) *Connection {
	connection := Connection{pending: make(chan *ClientMessage, 0),
		received:             make(chan *ClientMessage, 0),
		closed:               make(chan bool, 0),
		clientMessageBuilder: ClientMessageBuilder{responseChannel: responseChannel}, sendingError: sendingError}
	//go func() {
	socket, err := net.Dial("tcp", address.Host()+":"+strconv.Itoa(address.Port()))
	if err != nil {
		close(connection.closed)
		fmt.Println("CONNECTION IS CLOSED")
	} else {
		connection.socket = socket
	}
	socket.Write([]byte("CB2"))

	//}()
	go connection.process()
	go connection.read()
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
		//TODO:: implement this.
		for {
			select {
			case resp := <-connection.received:
				connection.clientMessageBuilder.OnMessage(resp)
			}
		}
	}()
}

func (connection *Connection) Send(clientMessage *ClientMessage) error {
	/*
		//Client message is not sent through the channel since this is a select/case clouse.
		select {
		case connection.pending <- clientMessage:
			return nil
		case <-connection.closed:
			return errors.New("Connection Closed.")
		}
	*/
	connection.pending <- clientMessage
	return nil
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
func (connection *Connection) read() {
	//TODO :: What if the size is bigger than 8192
	buf := make([]byte, 8192)
	for {
		n, err := connection.socket.Read(buf)
		if n == 0 {
			continue
		}
		if err != nil {
			//TODO:: Handle error
			connection.closed <- true
		}
		if n >= 8192 {
			fmt.Println("Buffer was too small for the read.")
		}
		resp := NewClientMessage(buf, 0)
		connection.received <- resp
	}

}

func (connection *Connection) Close() {
	if !atomic.CompareAndSwapInt32(&connection.status, 0, 1) {
		return
	}
	close(connection.closed)

}
