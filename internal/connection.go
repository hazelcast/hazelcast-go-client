// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"encoding/binary"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
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
	endpoint               atomic.Value
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

func NewConnection(address *Address, responseChannel chan *ClientMessage, sendingError chan int64, connectionId int64, connectionManager *ConnectionManager) *Connection {
	connection := Connection{pending: make(chan *ClientMessage, 1),
		received:             make(chan *ClientMessage, 1),
		closed:               make(chan bool, 1),
		clientMessageBuilder: &ClientMessageBuilder{responseChannel: responseChannel, incompleteMessages: make(map[int64]*ClientMessage)}, sendingError: sendingError,
		heartBeating:      true,
		readBuffer:        make([]byte, 0),
		connectionId:      connectionId,
		connectionManager: connectionManager,
	}
	connection.endpoint.Store(&Address{})
	socket, err := net.Dial("tcp", address.Host()+":"+strconv.Itoa(address.Port()))
	if err != nil {
		return nil
	} else {
		connection.socket = socket
	}
	connection.lastRead.Store(time.Now())
	connection.lastWrite.Store(time.Time{})             //initialization
	connection.lastHeartbeatReceived.Store(time.Time{}) //initialization
	connection.lastHeartbeatReceived.Store(time.Time{}) //initialization
	connection.closedTime.Store(time.Time{})            //initialization
	socket.Write([]byte("CB2"))
	go connection.writePool()
	go connection.read()
	return &connection
}

func (connection *Connection) IsAlive() bool {
	return atomic.LoadInt32(&connection.status) == 0
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

func (connection *Connection) Send(clientMessage *ClientMessage) bool {
	if !connection.IsAlive() {
		return false
	}
	select {
	case <-connection.closed:
		return false
	case connection.pending <- clientMessage:
		return true

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
		", connected server version=%s", connection.IsAlive(), connection.connectionId,
		connection.endpoint.Load().(*Address).Host(), connection.endpoint.Load().(*Address).Port(),
		connection.lastRead.Load().(time.Time).String(), connection.lastWrite.Load().(time.Time).String(),
		connection.closedTime.Load().(time.Time).String(), connection.lastHeartbeatRequested.Load().(time.Time).String(),
		connection.lastHeartbeatReceived.Load().(time.Time).String(), *connection.serverHazelcastVersion)
}
