// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"crypto/tls"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/config"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/timeutil"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/versionutil"
)

const (
	kb              = 1024
	bufferSize      = 128 * kb
	protocolStarter = "CP2"
	IntMask         = 0xffff
)

type Connection struct {
	pending                   chan *proto.ClientMessage
	received                  chan *proto.ClientMessage
	socket                    net.Conn
	clientMessageBuilder      *clientMessageBuilder
	closed                    chan struct{}
	endpoint                  atomic.Value
	status                    int32
	isOwnerConnection         bool
	lastRead                  atomic.Value
	lastWrite                 atomic.Value
	closedTime                atomic.Value
	readBuffer                []byte
	clientMessageReader       *clientMessageReader
	connectionID              int64
	connectionManager         connectionManager
	connectedServerVersion    int32
	connectedServerVersionStr string
	startTime                 int64
	logger                    logger.Logger
}

func newConnection(address core.Address, cm connectionManager, handleResponse func(interface{}),
	networkCfg *config.NetworkConfig, logger logger.Logger) (*Connection, error) {
	connection := createDefaultConnection(cm, handleResponse, logger)
	socket, err := connection.createSocket(networkCfg, address)
	if err != nil {
		return nil, err
	}
	connection.socket = socket
	connection.init()
	connection.clientMessageReader = newClientMessageReader()
	connection.sendProtocolStarter()
	go connection.writePool()
	go connection.readPool()
	return connection, nil
}

func createDefaultConnection(cm connectionManager, handleResponse func(interface{}), logger logger.Logger) *Connection {

	builder := &clientMessageBuilder{
		handleResponse:     handleResponse,
		incompleteMessages: make(map[int64]*proto.ClientMessage),
	}
	return &Connection{
		pending:              make(chan *proto.ClientMessage, 1),
		received:             make(chan *proto.ClientMessage, 1),
		closed:               make(chan struct{}),
		clientMessageBuilder: builder,
		readBuffer:           make([]byte, 0),
		connectionID:         cm.NextConnectionID(),
		connectionManager:    cm,
		status:               0,
		logger:               logger,
	}
}

func (c *Connection) sendProtocolStarter() {
	c.socket.Write([]byte(protocolStarter))
}

func (c *Connection) createSocket(networkCfg *config.NetworkConfig, address core.Address) (net.Conn, error) {
	conTimeout := timeutil.GetPositiveDurationOrMax(networkCfg.ConnectionTimeout())
	socket, err := c.dialToAddressWithTimeout(address, conTimeout)
	if err != nil {
		return nil, err
	}
	if networkCfg.SSLConfig().Enabled() {
		socket, err = c.openTLSConnection(networkCfg.SSLConfig(), socket)
	}
	return socket, err
}

func (c *Connection) dialToAddressWithTimeout(address core.Address, conTimeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", address.String(), conTimeout)
}

func (c *Connection) init() {
	c.lastWrite.Store(time.Time{})
	c.closedTime.Store(time.Time{})
	c.startTime = timeutil.GetCurrentTimeInMilliSeconds()
	c.lastRead.Store(time.Now())
}

func (c *Connection) openTLSConnection(sslCfg *config.SSLConfig, conn net.Conn) (net.Conn, error) {
	tlsCon := tls.Client(conn, sslCfg.Config)
	err := tlsCon.Handshake()
	return tlsCon, err
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
				c.clientMessageBuilder.handleResponse(request.GetCorrelationID())
			} else {
				c.lastWrite.Store(time.Now())
			}
		case <-c.closed:
			return
		}
	}
}

func (c *Connection) send(clientMessage *proto.ClientMessage) bool {
	select {
	case <-c.closed:
		return false
	case c.pending <- clientMessage:
		return true

	}
}

func (c *Connection) write(clientMessage *proto.ClientMessage) error {
	bytes := make([]byte, clientMessage.GetTotalLength())
	clientMessage.GetBytes(bytes)
	c.socket.Write(bytes)
	return nil
}

func (c *Connection) readPool() {
	for {
		c.socket.SetDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 4096)
		n, err := c.socket.Read(buf)
		if !c.isAlive() {
			return
		}
		if err != nil {
			if c.isTimeoutError(err) {
				continue
			}
			if err.Error() == "EOF" {
				continue
			}
			c.close(err)
			return
		}
		if n == 0 {
			continue
		}

		c.clientMessageReader.Append(bytes.NewBuffer(buf[:n]))
		c.receiveMessage()
		c.clientMessageReader.Reset()
	}
}

func (c *Connection) isTimeoutError(err error) bool {
	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}

func (c *Connection) StartTime() int64 {
	return c.startTime
}

func (c *Connection) receiveMessage() {
	clientMessage := c.clientMessageReader.Read()
	if clientMessage != nil && clientMessage.StartFrame.HasUnFragmentedMessageFlags() {
		c.clientMessageBuilder.onMessage(clientMessage)
	}
}

func (c *Connection) localAddress() net.Addr {
	return c.socket.LocalAddr()
}

func (c *Connection) setConnectedServerVersion(connectedServerVersion string) {
	c.connectedServerVersionStr = connectedServerVersion
	c.connectedServerVersion = versionutil.CalculateVersion(connectedServerVersion)
}

func (c *Connection) close(err error) {
	if !atomic.CompareAndSwapInt32(&c.status, 0, 1) {
		return
	}
	c.logger.Warn("Connection :", c, " closed, err: ", err)
	close(c.closed)
	c.socket.Close()
	c.closedTime.Store(time.Now())
	c.connectionManager.onConnectionClose(c, core.NewHazelcastTargetDisconnectedError(err.Error(), err))
}

func (c *Connection) String() string {
	return fmt.Sprintf("ClientConnection{"+
		"isAlive=%t"+
		", connectionID=%d"+
		", endpoint=%s"+
		", lastReadTime=%s"+
		", lastWriteTime=%s"+
		", closedTime=%s"+
		", connected server version=%s", c.isAlive(), c.connectionID,
		c.endpoint.Load().(core.Address),
		c.lastRead.Load().(time.Time), c.lastWrite.Load().(time.Time),
		c.closedTime.Load().(time.Time), c.connectedServerVersionStr)
}

type clientMessageReader struct {
	src           *bytes.Buffer
	readOffset    int
	clientMessage *proto.ClientMessage
	rwMutex       sync.RWMutex
}

func newClientMessageReader() *clientMessageReader {
	return &clientMessageReader{src: &bytes.Buffer{}, readOffset: -1}
}

func (c *clientMessageReader) Append(buffer *bytes.Buffer) {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	c.src = buffer
}

func (c *clientMessageReader) Read() *proto.ClientMessage {
	for {
		if c.readFrame() {
			if c.clientMessage.EndFrame.IsFinalFrame() {
				return c.clientMessage
			}
			c.readOffset = -1
		} else {
			return nil
		}
	}
}
func (c *clientMessageReader) readFrame() bool {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	// init internal buffer
	remaining := c.src.Len()
	if remaining < proto.SizeOfFrameLengthAndFlags {
		// we don't have even the frame length and flags ready
		return false
	}

	if c.readOffset == -1 {
		frameLength := binary.LittleEndian.Uint32(c.src.Next(proto.IntSizeInBytes))
		if frameLength < proto.SizeOfFrameLengthAndFlags {
			//TODO add exception
		}

		flags := binary.LittleEndian.Uint16(c.src.Bytes())
		c.src.Next(proto.ShortSizeInBytes)
		size := frameLength - proto.SizeOfFrameLengthAndFlags
		frame := proto.NewFrameWith(make([]byte, size), flags)
		if c.clientMessage == nil {
			c.clientMessage = proto.NewClientMessageForDecode(frame)
		} else {
			c.clientMessage.AddFrame(frame)
		}
		c.readOffset = 0
		if size == 0 {
			return true
		}
	}

	length := len(c.clientMessage.EndFrame.Content) - c.readOffset
	return c.accumulate(c.src, length)
}

func (c *clientMessageReader) accumulate(src *bytes.Buffer, length int) bool {
	remaining := src.Len()
	readLength := length
	if remaining < length {
		readLength = remaining
	}

	if readLength > 0 {
		end := c.readOffset + readLength
		for i := c.readOffset; i < end; i++ {
			c.clientMessage.EndFrame.Content[i], _ = src.ReadByte()
		}
		c.readOffset += readLength
		return readLength == length
	}

	return false
}

func (c *clientMessageReader) Reset() {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	c.clientMessage = nil
	c.readOffset = -1
}
