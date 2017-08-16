package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
	"log"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

const BUFFER_SIZE = 8192 * 2

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
	lastRead             time.Time
	heartBeating         bool
}

func NewConnection(address *Address, responseChannel chan *ClientMessage, sendingError chan int64) *Connection {
	connection := Connection{pending: make(chan *ClientMessage, 0),
		received:             make(chan *ClientMessage, 0),
		closed:               make(chan bool, 0),
		clientMessageBuilder: ClientMessageBuilder{responseChannel: responseChannel}, sendingError: sendingError,
		heartBeating: true,
	}
	go connection.process()
	//go func() {
	socket, err := net.Dial("tcp", address.Host()+":"+strconv.Itoa(address.Port()))
	if err != nil {
		connection.Close()
		log.Println("CONNECTION IS CLOSED")
		return nil
	} else {
		connection.socket = socket
	}
	connection.lastRead = time.Now()
	socket.Write([]byte("CB2"))
	//}()

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
				connection.Close()
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
	//TODO :: What if the size is bigger than 8192*2
	buf := make([]byte, BUFFER_SIZE)
	for {
		n, err := connection.socket.Read(buf)
		if err != nil {
			//TODO:: Handle error
			connection.Close()
		}
		if n == 0 {
			continue
		}

		if n >= BUFFER_SIZE {
			log.Println("Buffer was too small for the read.")
		}
		resp := NewClientMessage(buf, 0)
		connection.received <- resp
	}
}

func (connection *Connection) Close() {
	//TODO :: Should the status be 1 for alive and 0 when closed ?
	if !atomic.CompareAndSwapInt32(&connection.status, 0, 1) {
		return
	}
	close(connection.closed)
}
