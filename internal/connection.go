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
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol/bufutil"
)

const BufferSize = 8192 * 2

type Connection struct {
	pending                chan *protocol.ClientMessage
	received               chan *protocol.ClientMessage
	socket                 net.Conn
	clientMessageBuilder   *clientMessageBuilder
	closed                 chan struct{}
	endpoint               atomic.Value
	sendingError           chan int64
	status                 int32
	isOwnerConnection      bool
	lastRead               atomic.Value
	lastWrite              atomic.Value
	closedTime             atomic.Value
	lastHeartbeatRequested atomic.Value
	lastHeartbeatReceived  atomic.Value
	serverHazelcastVersion string
	heartBeating           bool
	readBuffer             []byte
	connectionID           int64
	connectionManager      *connectionManager
}

func newConnection(address core.Address, responseChannel chan *protocol.ClientMessage, sendingError chan int64,
	connectionID int64, connectionManager *connectionManager) *Connection {
	connection := Connection{pending: make(chan *protocol.ClientMessage, 1),
		clientMessageBuilder: &clientMessageBuilder{responseChannel: responseChannel,
			incompleteMessages: make(map[int64]*protocol.ClientMessage)}, sendingError: sendingError,
		received:          make(chan *protocol.ClientMessage, 1),
		closed:            make(chan struct{}),
		heartBeating:      true,
		readBuffer:        make([]byte, 0),
		connectionID:      connectionID,
		connectionManager: connectionManager,
	}
	connection.endpoint.Store(&protocol.Address{})
	socket, err := net.Dial("tcp", address.Host()+":"+strconv.Itoa(address.Port()))
	if err != nil {
		return nil
	}
	connection.socket = socket
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

func (c *Connection) isAlive() bool {
	return atomic.LoadInt32(&c.status) == 0
}

func (c *Connection) writePool() {
	//Writer process
	for {
		select {
		case request := <-c.pending:
			err := c.write(request)
			if err != nil {
				c.sendingError <- request.CorrelationID()
			}
			c.lastWrite.Store(time.Now())
		case <-c.closed:
			return
		}
	}
}

func (c *Connection) send(clientMessage *protocol.ClientMessage) bool {
	if !c.isAlive() {
		return false
	}
	select {
	case <-c.closed:
		return false
	case c.pending <- clientMessage:
		return true

	}
}

func (c *Connection) write(clientMessage *protocol.ClientMessage) error {
	remainingLen := len(clientMessage.Buffer)
	writeIndex := 0
	for remainingLen > 0 {
		writtenLen, err := c.socket.Write(clientMessage.Buffer[writeIndex:])
		if err != nil {
			return err
		}
		remainingLen -= writtenLen
		writeIndex += writtenLen
	}

	return nil
}

func (c *Connection) read() {
	buf := make([]byte, BufferSize)
	for {
		c.socket.SetDeadline(time.Now().Add(2 * time.Second))
		n, err := c.socket.Read(buf)

		select {
		// Check if the connection is closed
		case <-c.closed:
			return
		default:
			// If the connection is still open continue as normal
		}

		if err != nil {
			netErr, ok := err.(net.Error)
			// Check if we got a timeout error, if so we will try again instead of closing the connection
			if ok && netErr.Timeout() {
				continue
			}
			c.close(err)
			return
		}
		if n == 0 {
			continue
		}
		c.readBuffer = append(c.readBuffer, buf[:n]...)
		c.receiveMessage()
	}
}

func (c *Connection) receiveMessage() {
	c.lastRead.Store(time.Now())
	for len(c.readBuffer) > bufutil.Int32SizeInBytes {
		frameLength := binary.LittleEndian.Uint32(c.readBuffer[0:4])
		if frameLength > uint32(len(c.readBuffer)) {
			return
		}
		resp := protocol.NewClientMessage(c.readBuffer[:frameLength], 0)
		c.readBuffer = c.readBuffer[frameLength:]
		c.clientMessageBuilder.onMessage(resp)
	}
}

func (c *Connection) close(err error) {
	if !atomic.CompareAndSwapInt32(&c.status, 0, 1) {
		return
	}
	close(c.closed)
	c.closedTime.Store(time.Now())
	c.connectionManager.connectionClosed(c, err)
}

func (c *Connection) String() string {
	return fmt.Sprintf("ClientConnection{"+
		"isAlive=%t"+
		", connectionID=%d"+
		", endpoint=%s:%d"+
		", lastReadTime=%s"+
		", lastWriteTime=%s"+
		", closedTime=%s"+
		", lastHeartbeatRequested=%s"+
		", lastHeartbeatReceived=%s"+
		", connected server version=%s", c.isAlive(), c.connectionID,
		c.endpoint.Load().(*protocol.Address).Host(), c.endpoint.Load().(*protocol.Address).Port(),
		c.lastRead.Load().(time.Time).String(), c.lastWrite.Load().(time.Time).String(),
		c.closedTime.Load().(time.Time).String(), c.lastHeartbeatRequested.Load().(time.Time).String(),
		c.lastHeartbeatReceived.Load().(time.Time).String(), c.serverHazelcastVersion)
}
