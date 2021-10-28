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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/types"
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
	lastWrite                 atomic.Value
	closedTime                atomic.Value
	socket                    net.Conn
	bWriter                   *bufio.Writer
	endpoint                  atomic.Value
	logger                    ilogger.Logger
	lastRead                  atomic.Value
	clusterConfig             *pubcluster.Config
	eventDispatcher           *event.DispatchService
	pending                   chan invocation.Invocation
	invocationService         *invocation.Service
	doneCh                    chan struct{}
	connectedServerVersionStr string
	memberUUID                types.UUID
	connectionID              int64
	connectedServerVersion    int32
	status                    int32
}

func (c *Connection) ConnectionID() int64 {
	return c.connectionID
}

func (c *Connection) LocalAddr() string {
	return c.socket.LocalAddr().String()
}

func (c *Connection) Endpoint() pubcluster.Address {
	return c.endpoint.Load().(pubcluster.Address)
}

func (c *Connection) SetEndpoint(addr pubcluster.Address) {
	c.endpoint.Store(addr)
}

func (c *Connection) start(clusterCfg *pubcluster.Config, addr pubcluster.Address) error {
	if socket, err := c.createSocket(clusterCfg, addr); err != nil {
		return err
	} else {
		c.SetEndpoint(addr)
		c.socket = socket
		c.bWriter = bufio.NewWriterSize(socket, writeBufferSize)
		c.lastWrite.Store(time.Time{})
		c.closedTime.Store(time.Time{})
		c.lastRead.Store(time.Now())
		if err = c.sendProtocolStarter(); err != nil {
			// ignoring the socket close error
			_ = c.socket.Close()
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
	conTimeout := positiveDurationOrMax(time.Duration(clusterCfg.Network.ConnectionTimeout))
	if socket, err := c.dialToAddressWithTimeout(address, conTimeout); err != nil {
		return nil, err
	} else {
		if !c.clusterConfig.Network.SSL.Enabled {
			return socket, err
		}
		c.logger.Debug(func() string {
			return fmt.Sprintf("%d: SSL is enabled for connection", c.connectionID)
		})
		tlsCon := tls.Client(socket, c.clusterConfig.Network.SSL.TLSConfig())
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
		case inv, ok := <-c.pending:
			if !ok {
				return
			}
			req := inv.Request()
			err := c.write(req)
			// Note: Go lang spec guarantees that it's safe to call len()
			// on any number of goroutines without further synchronization.
			// See: https://golang.org/ref/spec#Channel_types
			if err == nil && len(c.pending) == 0 {
				// no pending messages exist, so flush the buffer
				err = c.bWriter.Flush()
			}
			if err != nil {
				c.logger.Errorf("cluster.Connection write error: %w", err)
				req = req.Copy()
				req.Err = ihzerrors.NewIOError("writing message", err)
				if respErr := c.invocationService.WriteResponse(req); respErr != nil {
					c.logger.Debug(func() string {
						return fmt.Sprintf("sending response: %s", err.Error())
					})
					// prevent respawning the connection
					err = nil
				}
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
		c.lastRead.Store(time.Now())
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
					if err := codec.DecodeError(clientMessage); err != nil {
						clientMessage.Err = wrapError(err)
					}
				}
				if err := c.invocationService.WriteResponse(clientMessage); err != nil {
					c.logger.Debug(func() string {
						return fmt.Sprintf("sending response: %s", err.Error())
					})
					c.close(nil)
					return
				}
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
	case c.pending <- inv:
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
	c.connectedServerVersion = calculateVersion(connectedServerVersion)
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
		c.isAlive(), c.connectionID, c.Endpoint(), c.lastRead.Load(), c.lastWrite.Load(), c.closedTime.Load(), c.connectedServerVersionStr)
}

func positiveDurationOrMax(duration time.Duration) time.Duration {
	if duration > 0 {
		return duration
	}
	return time.Duration(math.MaxInt64)
}

func wrapError(err *ihzerrors.ServerError) error {
	targetErr := convertErrorCodeToError(errorCode(err.ErrorCode))
	return ihzerrors.NewClientError(err.String(), err, targetErr)
}

func convertErrorCodeToError(code errorCode) error {
	switch code {
	case errorCodeUndefined:
		return hzerrors.ErrUndefined
	case errorCodeArrayIndexOutOfBounds:
		return hzerrors.ErrArrayIndexOutOfBounds
	case errorCodeArrayStore:
		return hzerrors.ErrArrayStore
	case errorCodeAuthentication:
		return hzerrors.ErrAuthentication
	case errorCodeCache:
		return hzerrors.ErrCache
	case errorCodeCacheLoader:
		return hzerrors.ErrCacheLoader
	case errorCodeCacheNotExists:
		return hzerrors.ErrCacheNotExists
	case errorCodeCacheWriter:
		return hzerrors.ErrCacheWriter
	case errorCodeCallerNotMember:
		return hzerrors.ErrCallerNotMember
	case errorCodeCancellation:
		return hzerrors.ErrCancellation
	case errorCodeClassCast:
		return hzerrors.ErrClassCast
	case errorCodeClassNotFound:
		return hzerrors.ErrClassNotFound
	case errorCodeConcurrentModification:
		return hzerrors.ErrConcurrentModification
	case errorCodeConfigMismatch:
		return hzerrors.ErrConfigMismatch
	case errorCodeDistributedObjectDestroyed:
		return hzerrors.ErrDistributedObjectDestroyed
	case errorCodeEOF:
		return hzerrors.ErrEOF
	case errorCodeEntryProcessor:
		return hzerrors.ErrEntryProcessor
	case errorCodeExecution:
		return hzerrors.ErrExecution
	case errorCodeHazelcast:
		return hzerrors.ErrHazelcast
	case errorCodeHazelcastInstanceNotActive:
		return hzerrors.ErrHazelcastInstanceNotActive
	case errorCodeHazelcastOverLoad:
		return hzerrors.ErrHazelcastOverLoad
	case errorCodeHazelcastSerialization:
		return hzerrors.ErrHazelcastSerialization
	case errorCodeIO:
		return hzerrors.ErrIO
	case errorCodeIllegalArgument:
		return hzerrors.ErrIllegalArgument
	case errorCodeIllegalAccessException:
		return hzerrors.ErrIllegalAccessException
	case errorCodeIllegalAccess:
		return hzerrors.ErrIllegalAccess
	case errorCodeIllegalMonitorState:
		return hzerrors.ErrIllegalMonitorState
	case errorCodeIllegalState:
		return hzerrors.ErrIllegalState
	case errorCodeIllegalThreadState:
		return hzerrors.ErrIllegalThreadState
	case errorCodeIndexOutOfBounds:
		return hzerrors.ErrIndexOutOfBounds
	case errorCodeInterrupted:
		return hzerrors.ErrInterrupted
	case errorCodeInvalidAddress:
		return hzerrors.ErrInvalidAddress
	case errorCodeInvalidConfiguration:
		return hzerrors.ErrInvalidConfiguration
	case errorCodeMemberLeft:
		return hzerrors.ErrMemberLeft
	case errorCodeNegativeArraySize:
		return hzerrors.ErrNegativeArraySize
	case errorCodeNoSuchElement:
		return hzerrors.ErrNoSuchElement
	case errorCodeNotSerializable:
		return hzerrors.ErrNotSerializable
	case errorCodeNilPointer:
		return hzerrors.ErrNilPointer
	case errorCodeOperationTimeout:
		return hzerrors.ErrOperationTimeout
	case errorCodePartitionMigrating:
		return hzerrors.ErrPartitionMigrating
	case errorCodeQuery:
		return hzerrors.ErrQuery
	case errorCodeQueryResultSizeExceeded:
		return hzerrors.ErrQueryResultSizeExceeded
	case errorCodeSplitBrainProtection:
		return hzerrors.ErrSplitBrainProtection
	case errorCodeReachedMaxSize:
		return hzerrors.ErrReachedMaxSize
	case errorCodeRejectedExecution:
		return hzerrors.ErrRejectedExecution
	case errorCodeResponseAlreadySent:
		return hzerrors.ErrResponseAlreadySent
	case errorCodeRetryableHazelcast:
		return hzerrors.ErrRetryableHazelcast
	case errorCodeRetryableIO:
		return hzerrors.ErrRetryableIO
	case errorCodeRuntime:
		return hzerrors.ErrRuntime
	case errorCodeSecurity:
		return hzerrors.ErrSecurity
	case errorCodeSocket:
		return hzerrors.ErrSocket
	case errorCodeStaleSequence:
		return hzerrors.ErrStaleSequence
	case errorCodeTargetDisconnected:
		return hzerrors.ErrTargetDisconnected
	case errorCodeTargetNotMember:
		return hzerrors.ErrTargetNotMember
	case errorCodeTimeout:
		return hzerrors.ErrTimeout
	case errorCodeTopicOverload:
		return hzerrors.ErrTopicOverload
	case errorCodeTransaction:
		return hzerrors.ErrTransaction
	case errorCodeTransactionNotActive:
		return hzerrors.ErrTransactionNotActive
	case errorCodeTransactionTimedOut:
		return hzerrors.ErrTransactionTimedOut
	case errorCodeURISyntax:
		return hzerrors.ErrURISyntax
	case errorCodeUTFDataFormat:
		return hzerrors.ErrUTFDataFormat
	case errorCodeUnsupportedOperation:
		return hzerrors.ErrUnsupportedOperation
	case errorCodeWrongTarget:
		return hzerrors.ErrWrongTarget
	case errorCodeXA:
		return hzerrors.ErrXA
	case errorCodeAccessControl:
		return hzerrors.ErrAccessControl
	case errorCodeLogin:
		return hzerrors.ErrLogin
	case errorCodeUnsupportedCallback:
		return hzerrors.ErrUnsupportedCallback
	case errorCodeNoDataMember:
		return hzerrors.ErrNoDataMember
	case errorCodeReplicatedMapCantBeCreated:
		return hzerrors.ErrReplicatedMapCantBeCreated
	case errorCodeMaxMessageSizeExceeded:
		return hzerrors.ErrMaxMessageSizeExceeded
	case errorCodeWANReplicationQueueFull:
		return hzerrors.ErrWANReplicationQueueFull
	case errorCodeAssertionError:
		return hzerrors.ErrAssertion
	case errorCodeOutOfMemoryError:
		return hzerrors.ErrOutOfMemory
	case errorCodeStackOverflowError:
		return hzerrors.ErrStackOverflow
	case errorCodeNativeOutOfMemoryError:
		return hzerrors.ErrNativeOutOfMemory
	case errorCodeServiceNotFound:
		return hzerrors.ErrServiceNotFound
	case errorCodeStaleTaskID:
		return hzerrors.ErrStaleTaskID
	case errorCodeDuplicateTask:
		return hzerrors.ErrDuplicateTask
	case errorCodeStaleTask:
		return hzerrors.ErrStaleTask
	case errorCodeLocalMemberReset:
		return hzerrors.ErrLocalMemberReset
	case errorCodeIndeterminateOperationState:
		return hzerrors.ErrIndeterminateOperationState
	case errorCodeFlakeIDNodeIDOutOfRangeException:
		return hzerrors.ErrFlakeIDNodeIDOutOfRangeException
	case errorCodeTargetNotReplicaException:
		return hzerrors.ErrTargetNotReplicaException
	case errorCodeMutationDisallowedException:
		return hzerrors.ErrMutationDisallowedException
	case errorCodeConsistencyLostException:
		return hzerrors.ErrConsistencyLostException
	case errorCodeSessionExpiredException:
		return hzerrors.ErrSessionExpiredException
	case errorCodeWaitKeyCancelledException:
		return hzerrors.ErrWaitKeyCancelledException
	case errorCodeLockAcquireLimitReachedException:
		return hzerrors.ErrLockAcquireLimitReachedException
	case errorCodeLockOwnershipLostException:
		return hzerrors.ErrLockOwnershipLostException
	case errorCodeCPGroupDestroyedException:
		return hzerrors.ErrCPGroupDestroyedException
	case errorCodeCannotReplicateException:
		return hzerrors.ErrCannotReplicateException
	case errorCodeLeaderDemotedException:
		return hzerrors.ErrLeaderDemotedException
	case errorCodeStaleAppendRequestException:
		return hzerrors.ErrStaleAppendRequestException
	case errorCodeNotLeaderException:
		return hzerrors.ErrNotLeaderException
	case errorCodeVersionMismatchException:
		return hzerrors.ErrVersionMismatchException
	case errorCodeNoSuchMethod:
		return hzerrors.ErrNoSuchMethod
	case errorCodeNoSuchMethodException:
		return hzerrors.ErrNoSuchMethodException
	case errorCodeNoSuchField:
		return hzerrors.ErrNoSuchField
	case errorCodeNoSuchFieldException:
		return hzerrors.ErrNoSuchFieldException
	case errorCodeNoClassDefFound:
		return hzerrors.ErrNoClassDefFound
	}
	return nil
}

const (
	unknownVersion         int32 = -1
	majorVersionMultiplier int32 = 10000
	minorVersionMultiplier int32 = 100
)

func calculateVersion(version string) int32 {
	if version == "" {
		return unknownVersion
	}
	mainParts := strings.Split(version, "-")
	tokens := strings.Split(mainParts[0], ".")
	if len(tokens) < 2 {
		return unknownVersion
	}
	majorCoeff, err := strconv.Atoi(tokens[0])
	if err != nil {
		return unknownVersion
	}
	minorCoeff, err := strconv.Atoi(tokens[1])
	if err != nil {
		return unknownVersion
	}
	calculatedVersion := int32(majorCoeff) * majorVersionMultiplier
	calculatedVersion += int32(minorCoeff) * minorVersionMultiplier
	if len(tokens) > 2 {
		lastCoeff, err := strconv.Atoi(tokens[2])
		if err != nil {
			return unknownVersion
		}
		calculatedVersion += int32(lastCoeff)
	}
	return calculatedVersion
}
