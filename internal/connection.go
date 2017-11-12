package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

const BUFFER_SIZE = 8192 * 2

type Connection struct {
	pending                chan *ClientMessage
	received               chan *ClientMessage
	socket                 net.Conn
	clientMessageBuilder   *ClientMessageBuilder
	closed                 chan bool
	endpoint               *Address
	sendingError           chan int64
	status                 int32
	isOwnerConnection      bool
	lastRead               atomic.Value
	lastWrite              atomic.Value
	closedTime             atomic.Value
	lastHeartbeatRequested atomic.Value
	lastHeartbeatReceived  atomic.Value
	serverHazelcastVersion *string
	heartBeating           bool
	readBuffer             []byte
	connectionId           int64
	connectionManager      *ConnectionManager
}

func NewConnection(address *Address, responseChannel chan *ClientMessage, sendingError chan int64, connectionManager *ConnectionManager, connectionId int64) *Connection {
	connection := Connection{pending: make(chan *ClientMessage, 1),
		received:             make(chan *ClientMessage, 0),
		closed:               make(chan bool, 0),
		clientMessageBuilder: &ClientMessageBuilder{responseChannel: responseChannel, incompleteMessages: make(map[int64]*ClientMessage)}, sendingError: sendingError,
		connectionManager: connectionManager,
		heartBeating:      true,
		readBuffer:        make([]byte, 0),
		connectionId:      connectionId,
	}
	socket, err := net.Dial("tcp", address.Host()+":"+strconv.Itoa(address.Port()))
	if err != nil {
		connection.Close(err)
		return nil
	} else {
		connection.socket = socket
	}
	connection.lastRead.Store(time.Now())
	socket.Write([]byte("CB2"))
	go connection.writePool()
	go connection.read()
	return &connection
}
func (connection *Connection) IsConnected() bool {
	return connection.socket != nil && connection.socket.RemoteAddr() != nil
}
func (connection *Connection) IsAlive() bool {
	return connection.status == 0
}
func (connection *Connection) writePool() {
	//Writer process
	for {
		select {
		case request := <-connection.pending:
			err := connection.write(request)
			if err != nil {
				connection.sendingError <- request.CorrelationId()
			}
			connection.lastWrite.Store(time.Now())
		case <-connection.closed:
			return
		}
	}
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
func (connection *Connection) read() {
	buf := make([]byte, BUFFER_SIZE)
	for {
		n, err := connection.socket.Read(buf)
		connection.readBuffer = append(connection.readBuffer, buf[:n]...)
		if err != nil {
			connection.Close(err)
			return
		}
		if n == 0 {
			continue
		}
		connection.receiveMessage()
	}
}
func (connection *Connection) receiveMessage() {
	connection.lastRead.Store(time.Now())
	for len(connection.readBuffer) > common.INT_SIZE_IN_BYTES {
		frameLength := binary.LittleEndian.Uint32(connection.readBuffer[0:4])
		if frameLength > uint32(len(connection.readBuffer)) {
			return
		}
		resp := NewClientMessage(connection.readBuffer[:frameLength], 0)
		connection.readBuffer = connection.readBuffer[frameLength:]
		connection.clientMessageBuilder.OnMessage(resp)
	}
}
func (connection *Connection) Close(err error) {
	//TODO :: Should the status be 1 for alive and 0 when closed ?
	if !atomic.CompareAndSwapInt32(&connection.status, 0, 1) {
		return
	}
	close(connection.closed)
	connection.closedTime.Store(time.Now())
	connection.connectionManager.connectionClosed(connection, err)

}

func (connection *Connection) String() string {
	return fmt.Sprintf("ClientConnection{"+
		"isAlive=%t"+
		", connectionId=%d"+
		", endpoint=%s:%d"+
		", lastReadTime=%s"+
		", lastWriteTime=%s"+
		", closedTime=%s"+
		", lastHeartbeatRequested=%s"+
		", lastHeartbeatReceived=%s"+
		", connected server version=%s", connection.IsAlive(), connection.connectionId, connection.endpoint.Host(), connection.endpoint.Port(),
		connection.lastRead.Load().(time.Time).String(), connection.lastWrite.Load().(time.Time).String(),
		connection.closedTime.Load().(time.Time).String(), connection.lastHeartbeatRequested.Load().(time.Time).String(),
		connection.lastHeartbeatReceived.Load().(time.Time).String(), *connection.serverHazelcastVersion)
}
