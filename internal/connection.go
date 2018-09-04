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
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/timeutil"
)

const BufferSize = 8192 * 2

type Connection struct {
	pending                chan *proto.ClientMessage
	received               chan *proto.ClientMessage
	socket                 net.Conn
	clientMessageBuilder   *clientMessageBuilder
	closed                 chan struct{}
	endpoint               atomic.Value
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
	connectionManager      connectionManager
}

func newConnection(client *HazelcastClient, address core.Address, handleResponse func(interface{}),
	connectionID int64, connectionManager connectionManager) (*Connection, error) {
	builder := &clientMessageBuilder{handleResponse: handleResponse,
		incompleteMessages: make(map[int64]*proto.ClientMessage)}
	connection := Connection{pending: make(chan *proto.ClientMessage, 1),
		received:             make(chan *proto.ClientMessage, 1),
		closed:               make(chan struct{}),
		clientMessageBuilder: builder,
		heartBeating:         true,
		readBuffer:           make([]byte, 0),
		connectionID:         connectionID,
		connectionManager:    connectionManager,
		status:               0,
	}
	connectionTimeout := timeutil.GetPositiveDurationOrMax(client.ClientConfig.NetworkConfig().ConnectionTimeout())
	socket, err := net.DialTimeout("tcp", address.String(), connectionTimeout)
	if err != nil {
		return nil, err
	}
	connection.socket = socket
	connection.lastRead.Store(time.Now())
	connection.lastWrite.Store(time.Time{})              //initialization
	connection.lastHeartbeatReceived.Store(time.Time{})  //initialization
	connection.lastHeartbeatRequested.Store(time.Time{}) //initialization
	connection.closedTime.Store(time.Time{})             //initialization
	socket.Write([]byte("CB2"))
	go connection.writePool()
	go connection.read()
	return &connection, nil
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
				c.clientMessageBuilder.handleResponse(request.CorrelationID())
			}
			c.lastWrite.Store(time.Now())
		case <-c.closed:
			return
		}
	}
}

func (c *Connection) send(clientMessage *proto.ClientMessage) bool {
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

func (c *Connection) write(clientMessage *proto.ClientMessage) error {
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
		resp := proto.NewClientMessage(c.readBuffer[:frameLength], 0)
		c.readBuffer = c.readBuffer[frameLength:]
		c.clientMessageBuilder.onMessage(resp)
	}
}

func (c *Connection) close(err error) {
	if !atomic.CompareAndSwapInt32(&c.status, 0, 1) {
		return
	}
	//TODO log close message
	close(c.closed)
	c.closedTime.Store(time.Now())
	c.connectionManager.onConnectionClose(c, err)
}

func (c *Connection) String() string {
	return fmt.Sprintf("ClientConnection{"+
		"isAlive=%t"+
		", connectionID=%d"+
		", endpoint=%s"+
		", lastReadTime=%s"+
		", lastWriteTime=%s"+
		", closedTime=%s"+
		", lastHeartbeatRequested=%s"+
		", lastHeartbeatReceived=%s"+
		", connected server version=%s", c.isAlive(), c.connectionID,
		c.endpoint.Load().(core.Address),
		c.lastRead.Load().(time.Time), c.lastWrite.Load().(time.Time),
		c.closedTime.Load().(time.Time), c.lastHeartbeatRequested.Load().(time.Time),
		c.lastHeartbeatReceived.Load().(time.Time), c.serverHazelcastVersion)
}
