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

package connection

import (
	"bytes"
	"fmt"
	"net"
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

type Impl struct {
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
	connectionManager         Manager
	connectedServerVersion    int32
	connectedServerVersionStr string
	startTime                 int64
	logger                    logger.Logger
}

func newConnection(address *core.Address, cm Manager, handleResponse func(interface{}),
	networkCfg *config.NetworkConfig, logger logger.Logger) (*Impl, error) {
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

func createDefaultConnection(cm Manager, handleResponse func(interface{}), logger logger.Logger) *Impl {
	builder := &clientMessageBuilder{
		handleResponse:     handleResponse,
		incompleteMessages: make(map[int64]*proto.ClientMessage),
	}
	return &Impl{
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

func (c *Impl) sendProtocolStarter() {
	c.socket.Write([]byte(protocolStarter))
}

func (c *Impl) createSocket(networkCfg *config.NetworkConfig, address *core.Address) (net.Conn, error) {
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

func (c *Impl) dialToAddressWithTimeout(address *core.Address, conTimeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", address.String(), conTimeout)
}

func (c *Impl) init() {
	c.lastWrite.Store(time.Time{})
	c.closedTime.Store(time.Time{})
	c.startTime = timeutil.GetCurrentTimeInMilliSeconds()
	c.lastRead.Store(time.Now())
}

func (c *Impl) openTLSConnection(sslCfg *config.SSLConfig, conn net.Conn) (net.Conn, error) {
	tlsCon := tls.Client(conn, sslCfg.Config)
	err := tlsCon.Handshake()
	return tlsCon, err
}

func (c *Impl) isAlive() bool {
	return atomic.LoadInt32(&c.status) == 0
}

func (c *Impl) writePool() {
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

func (c *Impl) send(clientMessage *proto.ClientMessage) bool {
	select {
	case <-c.closed:
		return false
	case c.pending <- clientMessage:
		return true
	}
}

func (c *Impl) write(clientMessage *proto.ClientMessage) error {
	bytes := make([]byte, clientMessage.GetTotalLength())
	clientMessage.GetBytes(bytes)
	c.socket.Write(bytes)
	return nil
}

func (c *Impl) readPool() {
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

func (c *Impl) isTimeoutError(err error) bool {
	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}

func (c *Impl) StartTime() int64 {
	return c.startTime
}

func (c *Impl) receiveMessage() {
	clientMessage := c.clientMessageReader.Read()
	if clientMessage != nil && clientMessage.StartFrame.HasUnFragmentedMessageFlags() {
		c.clientMessageBuilder.onMessage(clientMessage)
	}
}

func (c *Impl) localAddress() net.Addr {
	return c.socket.LocalAddr()
}

func (c *Impl) setConnectedServerVersion(connectedServerVersion string) {
	c.connectedServerVersionStr = connectedServerVersion
	c.connectedServerVersion = versionutil.CalculateVersion(connectedServerVersion)
}

func (c *Impl) close(err error) {
	if !atomic.CompareAndSwapInt32(&c.status, 0, 1) {
		return
	}
	c.logger.Warn("Impl :", c, " closed, err: ", err)
	close(c.closed)
	c.socket.Close()
	c.closedTime.Store(time.Now())
	c.connectionManager.notifyConnectionClosed(c, core.NewHazelcastTargetDisconnectedError(err.Error(), err))
}

func (c *Impl) String() string {
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
