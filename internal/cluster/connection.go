/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/util/versionutil"
)

const (
	socketBufferSize     = 128 * 1024
	writeBufferSize      = 16 * 1024
	protocolStarter      = "CP2"
	messageTypeException = int32(0)
)

const (
	open int32 = iota
	closed
)

type ResponseHandler func(msg *proto.ClientMessage)

type Connection struct {
	lastRead                  atomic.Value
	lastWrite                 atomic.Value
	closedTime                atomic.Value
	socket                    net.Conn
	bWriter                   *bufio.Writer
	endpoint                  atomic.Value
	logger                    ilogger.Logger
	eventDispatcher           *event.DispatchService
	pending                   chan *proto.ClientMessage
	doneCh                    chan struct{}
	responseCh                chan<- *proto.ClientMessage
	clusterConfig             *pubcluster.Config
	connectedServerVersionStr string
	connectionID              int64
	connectedServerVersion    int32
	status                    int32
}

func (c *Connection) ConnectionID() int64 {
	return c.connectionID
}

func (c *Connection) Endpoint() *pubcluster.AddressImpl {
	return c.endpoint.Load().(*pubcluster.AddressImpl)
}

func (c *Connection) start(clusterCfg *pubcluster.Config, addr pubcluster.Address) error {
	if socket, err := c.createSocket(clusterCfg, addr); err != nil {
		return err
	} else {
		c.socket = socket
		c.bWriter = bufio.NewWriterSize(socket, writeBufferSize)
		c.lastWrite.Store(time.Time{})
		c.closedTime.Store(time.Time{})
		c.lastRead.Store(time.Now())
		if err := c.sendProtocolStarter(); err != nil {
			c.socket.Close()
			c.socket = nil
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

func (c *Connection) createSocket(clusterCfg *pubcluster.Config, address pubcluster.Address) (net.Conn, error) {
	conTimeout := positiveDurationOrMax(clusterCfg.ConnectionTimeout)
	if socket, err := c.dialToAddressWithTimeout(address, conTimeout); err != nil {
		return nil, err
	} else {
		if !c.clusterConfig.SSLConfig.Enabled {
			return socket, err
		}
		c.logger.Debug(func() string {
			return fmt.Sprintf("%d: SSL is enabled for connection", c.connectionID)
		})
		tlsCon := tls.Client(socket, c.clusterConfig.SSLConfig.TLSConfig)
		if err = tlsCon.Handshake(); err != nil {
			return nil, err
		}
		return tlsCon, nil
	}
}

func (c *Connection) dialToAddressWithTimeout(addr pubcluster.Address, conTimeout time.Duration) (*net.TCPConn, error) {
	if conn, err := net.DialTimeout("tcp", addr.String(), conTimeout); err != nil {
		return nil, err
	} else {
		tcpConn := conn.(*net.TCPConn)
		if err = tcpConn.SetNoDelay(false); err != nil {
			c.logger.Warnf("error setting tcp no delay: %w", err)
		}
		if err = tcpConn.SetReadBuffer(socketBufferSize); err != nil {
			c.logger.Warnf("error setting read buffer: %w", err)
		}
		if err = tcpConn.SetWriteBuffer(socketBufferSize); err != nil {
			c.logger.Warnf("error setting write buffer: %w", err)
		}
		return tcpConn, nil
	}
}

func (c *Connection) isAlive() bool {
	return atomic.LoadInt32(&c.status) == open
}

func (c *Connection) socketWriteLoop() {
	for {
		select {
		case request, ok := <-c.pending:
			if !ok {
				return
			}
			err := c.write(request)
			// Note: Go lang spec guarantees that it's safe to call len()
			// on any number of goroutines without further synchronization.
			// See: https://golang.org/ref/spec#Channel_types
			if err == nil && len(c.pending) == 0 {
				// no pending messages exist, so flush the buffer
				err = c.bWriter.Flush()
			}
			if err != nil {
				c.logger.Errorf("write error: %w", err)
				request = request.Copy()
				request.Err = hzerrors.NewHazelcastIOError("writing message", err)
				c.responseCh <- request
				c.close(err)
			} else {
				c.lastWrite.Store(time.Now())
			}
		case <-c.doneCh:
			return
		}
	}
}

func (c *Connection) socketReadLoop() {
	var err error
	var n int
	buf := make([]byte, socketBufferSize)
	clientMessageReader := newClientMessageReader()
	for {
		if err := c.socket.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
			break
		}
		n, err = c.socket.Read(buf)
		if atomic.LoadInt32(&c.status) != open {
			break
		}
		if err != nil {
			if c.isTimeoutError(err) {
				continue
			}
			if err != io.EOF {
				c.logger.Errorf("read error: %w", err)
			}
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
			if clientMessage.HasUnFragmentedMessageFlags() {
				c.logger.Trace(func() string {
					return fmt.Sprintf("%d: read invocation with correlation ID: %d", c.connectionID, clientMessage.CorrelationID())
				})
				if clientMessage.Type() == messageTypeException {
					clientMessage.Err = hzerrors.NewHazelcastError(codec.DecodeError(clientMessage))
				}
				c.responseCh <- clientMessage
				clientMessageReader.ResetMessage()
			}
		}
		clientMessageReader.ResetBuffer()
	}
	c.close(err)
}

func (c *Connection) send(inv invocation.Invocation) bool {
	select {
	case <-c.doneCh:
		return false
	case c.pending <- inv.Request():
		//inv.StoreSentConnection(c)
		return true
	}
}

func (c *Connection) write(clientMessage *proto.ClientMessage) error {
	c.logger.Trace(func() string {
		return fmt.Sprintf("%d: writing invocation with correlation ID: %d", c.connectionID, clientMessage.CorrelationID())
	})
	return clientMessage.Write(c.bWriter)
}

func (c *Connection) isTimeoutError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

func (c *Connection) setConnectedServerVersion(connectedServerVersion string) {
	c.connectedServerVersionStr = connectedServerVersion
	c.connectedServerVersion = versionutil.CalculateVersion(connectedServerVersion)
}

func (c *Connection) close(closeErr error) {
	if !atomic.CompareAndSwapInt32(&c.status, open, closed) {
		return
	}
	close(c.doneCh)
	c.socket.Close()
	c.closedTime.Store(time.Now())
	c.eventDispatcher.Publish(NewConnectionClosed(c, closeErr))
	c.logger.Trace(func() string {
		reason := "normally"
		if closeErr != nil {
			reason = fmt.Sprintf("reason: %s", closeErr.Error())
		}
		return fmt.Sprintf("%d: connection closed %s", c.connectionID, reason)
	})
}

func (c *Connection) String() string {
	return fmt.Sprintf("ClientConnection{isAlive=%t, connectionID=%d, endpoint=%s, lastReadTime=%s, lastWriteTime=%s, closedTime=%s, connected server version=%s",
		c.isAlive(), c.connectionID, c.endpoint.Load(), c.lastRead.Load(), c.lastWrite.Load(), c.closedTime.Load(), c.connectedServerVersionStr)
}

func positiveDurationOrMax(duration time.Duration) time.Duration {
	if duration > 0 {
		return duration
	}
	return time.Duration(math.MaxInt64)
}
