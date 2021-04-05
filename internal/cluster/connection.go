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

package cluster

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"

	publogger "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/timeutil"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/versionutil"
)

const (
	bufferSize      = 128 * 1024
	protocolStarter = "CP2"
)

type ResponseHandler func(msg *proto.ClientMessage)

type Connection struct {
	responseCh                chan<- *proto.ClientMessage
	pending                   chan *proto.ClientMessage
	received                  chan *proto.ClientMessage
	socket                    net.Conn
	closed                    chan struct{}
	endpoint                  atomic.Value
	status                    int32
	isOwnerConnection         bool
	lastRead                  atomic.Value
	lastWrite                 atomic.Value
	closedTime                atomic.Value
	readBuffer                []byte
	connectionID              int64
	eventDispatcher           event.DispatchService
	connectedServerVersion    int32
	connectedServerVersionStr string
	startTime                 int64
	logger                    publogger.Logger
}

func (c *Connection) ConnectionID() int64 {
	return c.connectionID
}

func (c *Connection) start(clusterCfg *pubcluster.Config, addr pubcluster.Address) error {
	if socket, err := c.createSocket(clusterCfg, addr); err != nil {
		return err
	} else {
		c.socket = socket
		c.init()
		if err := c.sendProtocolStarter(); err != nil {
			return err
		}
		go c.socketReadLoop()
		go c.socketWriteLoop()
		return nil
	}
}

func (c *Connection) sendProtocolStarter() error {
	_, err := c.socket.Write([]byte(protocolStarter))
	return err
}

func (c *Connection) createSocket(networkCfg *pubcluster.Config, address pubcluster.Address) (net.Conn, error) {
	conTimeout := timeutil.GetPositiveDurationOrMax(networkCfg.ConnectionTimeout)
	socket, err := c.dialToAddressWithTimeout(address, conTimeout)
	if err != nil {
		return nil, err
	}
	//if networkCfg.SSLConfig().Enabled() {
	//	socket, err = c.openTLSConnection(networkCfg.SSLConfig(), socket)
	//}
	return socket, err
}

func (c *Connection) dialToAddressWithTimeout(addr pubcluster.Address, conTimeout time.Duration) (*net.TCPConn, error) {
	if tcpAddr, err := net.ResolveTCPAddr("tcp", addr.String()); err != nil {
		return nil, err
	} else if conn, err := net.DialTCP("tcp", nil, tcpAddr); err != nil {
		return nil, err
	} else {
		conn.SetNoDelay(true)
		conn.SetReadBuffer(bufferSize)
		conn.SetWriteBuffer(bufferSize)
		return conn, nil
	}
}

func (c *Connection) init() {
	c.lastWrite.Store(time.Time{})
	c.closedTime.Store(time.Time{})
	c.startTime = timeutil.GetCurrentTimeInMilliSeconds()
	c.lastRead.Store(time.Now())
}

func (c *Connection) isAlive() bool {
	return atomic.LoadInt32(&c.status) == 0
}

func (c *Connection) socketWriteLoop() {
	for {
		select {
		case request, ok := <-c.pending:
			if !ok {
				return
			}
			if err := c.write(request); err != nil {
				c.logger.Errorf("write error: %w", err)
				// XXX: create a new client message?
				request.Err = err
				c.responseCh <- request
			} else {
				c.lastWrite.Store(time.Now())
			}
		case <-c.closed:
			return
		}
	}
}

func (c *Connection) socketReadLoop() {
	var err error
	var n int
	buf := make([]byte, bufferSize)
	clientMessageReader := newClientMessageReader()
	for {
		c.socket.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, err = c.socket.Read(buf)
		if !c.isAlive() {
			return
		}
		if err != nil {
			if c.isTimeoutError(err) {
				continue
			}
			c.logger.Errorf("read error: %w", err)
			break
		}
		if n == 0 {
			continue
		}
		clientMessageReader.Append(buf[:n])
		for {
			clientMessage := clientMessageReader.Read()
			if clientMessage == nil {
				break
			}
			if clientMessage.StartFrame.HasUnFragmentedMessageFlags() {
				c.responseCh <- clientMessage
			}
			clientMessageReader.Reset()
		}

	}
	c.close(err)
}

func (c *Connection) send(inv invocation.Invocation) bool {
	select {
	case <-c.closed:
		return false
	case c.pending <- inv.Request():
		//inv.StoreSentConnection(c)
		return true
	}
}

func (c *Connection) write(clientMessage *proto.ClientMessage) error {
	buf := make([]byte, clientMessage.TotalLength())
	clientMessage.Bytes(buf)
	c.socket.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
	_, err := c.socket.Write(buf)
	return err
}

func (c *Connection) isTimeoutError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

func (c *Connection) StartTime() int64 {
	return c.startTime
}

func (c *Connection) localAddress() net.Addr {
	return c.socket.LocalAddr()
}

func (c *Connection) setConnectedServerVersion(connectedServerVersion string) {
	c.connectedServerVersionStr = connectedServerVersion
	c.connectedServerVersion = versionutil.CalculateVersion(connectedServerVersion)
}

func (c *Connection) close(closeErr error) {
	if !atomic.CompareAndSwapInt32(&c.status, 0, 1) {
		return
	}
	close(c.closed)
	c.socket.Close()
	c.closedTime.Store(time.Now())
	c.eventDispatcher.Publish(NewConnectionClosed(c, closeErr))
}

func (c *Connection) String() string {
	return fmt.Sprintf("ClientConnection{isAlive=%t, connectionID=%d, endpoint=%s, lastReadTime=%s, lastWriteTime=%s, closedTime=%s, connected server version=%s",
		c.isAlive(), c.connectionID, c.endpoint.Load(), c.lastRead.Load(), c.lastWrite.Load(), c.closedTime.Load(), c.connectedServerVersionStr)
}
