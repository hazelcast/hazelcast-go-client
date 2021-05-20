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
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/security"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/util/nilutil"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	authenticated = iota
	credentialsFailed
	serializationVersionMismatch
)

const (
	created int32 = iota
	starting
	ready
	stopping
	stopped
)

const serializationVersion = 1

// ClientVersion is the build time version
// TODO: This should be replace with a build time version variable, BuildInfo etc.
var ClientVersion = "1.0.0"

type ConnectionManagerCreationBundle struct {
	RequestCh            chan<- invocation.Invocation
	ResponseCh           chan<- *proto.ClientMessage
	Logger               ilogger.Logger
	ClusterService       *Service
	PartitionService     *PartitionService
	SerializationService *iserialization.Service
	EventDispatcher      *event.DispatchService
	InvocationFactory    *ConnectionInvocationFactory
	ClusterConfig        *pubcluster.Config
	Credentials          security.Credentials
	ClientName           string
}

func (b ConnectionManagerCreationBundle) Check() {
	if b.RequestCh == nil {
		panic("RequestCh is nil")
	}
	if b.ResponseCh == nil {
		panic("ResponseCh is nil")
	}
	if b.Logger == nil {
		panic("Logger is nil")
	}
	if b.ClusterService == nil {
		panic("ClusterService is nil")
	}
	if b.PartitionService == nil {
		panic("PartitionService is nil")
	}
	if b.SerializationService == nil {
		panic("SerializationService is nil")
	}
	if b.EventDispatcher == nil {
		panic("EventDispatcher is nil")
	}
	if b.InvocationFactory == nil {
		panic("InvocationFactory is nil")
	}
	if b.ClusterConfig == nil {
		panic("ClusterConfig is nil")
	}
	if b.Credentials == nil {
		panic("Credentials is nil")
	}
	if b.ClientName == "" {
		panic("ClientName is blank")
	}
}

type ConnectionManager struct {
	requestCh            chan<- invocation.Invocation
	responseCh           chan<- *proto.ClientMessage
	clusterService       *Service
	partitionService     *PartitionService
	serializationService *iserialization.Service
	eventDispatcher      *event.DispatchService
	invocationFactory    *ConnectionInvocationFactory
	clusterConfig        *pubcluster.Config
	credentials          security.Credentials
	clientName           string
	clientUUID           types.UUID
	connMap              *connectionMap
	nextConnID           int64
	addressTranslator    AddressTranslator
	smartRouting         bool
	state                int32
	logger               ilogger.Logger
	doneCh               chan struct{}
	startCh              chan struct{}
	cb                   *cb.CircuitBreaker
}

func NewConnectionManager(bundle ConnectionManagerCreationBundle) *ConnectionManager {
	bundle.Check()
	// TODO: make circuit breaker configurable
	circuitBreaker := cb.NewCircuitBreaker(
		cb.MaxRetries(math.MaxInt32),
		cb.MaxFailureCount(3),
		cb.RetryPolicy(func(attempt int) time.Duration {
			if attempt < 10 {
				return time.Duration((attempt+1)*100) * time.Millisecond
			}
			if attempt < 1000 {
				return time.Duration(attempt*attempt) * time.Millisecond
			}
			return 20 * time.Minute
		}))
	manager := &ConnectionManager{
		requestCh:            bundle.RequestCh,
		responseCh:           bundle.ResponseCh,
		clusterService:       bundle.ClusterService,
		partitionService:     bundle.PartitionService,
		serializationService: bundle.SerializationService,
		eventDispatcher:      bundle.EventDispatcher,
		invocationFactory:    bundle.InvocationFactory,
		clusterConfig:        bundle.ClusterConfig,
		credentials:          bundle.Credentials,
		clientName:           bundle.ClientName,
		clientUUID:           types.NewUUID(),
		connMap:              newConnectionMap(),
		addressTranslator:    NewDefaultAddressTranslator(),
		smartRouting:         bundle.ClusterConfig.SmartRouting,
		logger:               bundle.Logger,
		doneCh:               make(chan struct{}, 1),
		startCh:              make(chan struct{}, 1),
		cb:                   circuitBreaker,
	}
	return manager
}

func (m *ConnectionManager) Start(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&m.state, created, starting) {
		return nil
	}
	if _, err := m.cb.Try(func(ctx context.Context, attempt int) (interface{}, error) {
		err := m.connectCluster()
		if err != nil {
			m.logger.Errorf("error starting: %w", err)
		}
		return nil, err
	}); err != nil {
		return err
	}
	connectionClosedSubscriptionID := event.MakeSubscriptionID(m.handleConnectionClosed)
	m.eventDispatcher.Subscribe(EventConnectionClosed, connectionClosedSubscriptionID, m.handleConnectionClosed)
	m.eventDispatcher.Subscribe(EventMembersAdded, event.DefaultSubscriptionID, m.handleInitialMembersAdded)
	// wait for the initial member list
	select {
	case <-m.startCh:
		break
	case <-time.After(timeout):
		return errors.New("initial member list not received in deadline")
	}
	if m.smartRouting {
		// fix broken connections only in the smart mode
		go m.detectFixBrokenConnections()
	}
	if m.logger.CanLogDebug() {
		go m.logStatus()
	}
	atomic.StoreInt32(&m.state, ready)
	m.eventDispatcher.Publish(NewConnected())
	return nil
}

func (m *ConnectionManager) Stop() {
	if !atomic.CompareAndSwapInt32(&m.state, ready, stopping) {
		close(m.doneCh)
		return
	}
	m.eventDispatcher.Unsubscribe(EventConnectionClosed, event.MakeSubscriptionID(m.handleConnectionClosed))
	m.eventDispatcher.Unsubscribe(EventMembersAdded, event.MakeSubscriptionID(m.handleMembersAdded))
	m.connMap.CloseAll()
	atomic.StoreInt32(&m.state, stopped)
	close(m.doneCh)
}

func (m *ConnectionManager) NextConnectionID() int64 {
	return atomic.AddInt64(&m.nextConnID, 1)
}

func (m *ConnectionManager) GetConnectionForAddress(addr *pubcluster.AddressImpl) *Connection {
	return m.connMap.GetConnectionForAddr(addr.String())
}

func (m *ConnectionManager) GetConnectionForPartition(partitionID int32) *Connection {
	if partitionID < 0 {
		panic("partition ID is negative")
	}
	if ownerUUID := m.partitionService.GetPartitionOwner(partitionID); ownerUUID == nil {
		return nil
	} else if member := m.clusterService.GetMemberByUUID(ownerUUID.String()); nilutil.IsNil(member) {
		return nil
	} else {
		return m.GetConnectionForAddress(member.Address().(*pubcluster.AddressImpl))
	}
}

func (m *ConnectionManager) ActiveConnections() []*Connection {
	return m.connMap.Connections()
}

func (m *ConnectionManager) RandomConnection() *Connection {
	return m.connMap.RandomConn()
}

func (m *ConnectionManager) handleInitialMembersAdded(e event.Event) {
	m.handleMembersAdded(e)
	m.eventDispatcher.Subscribe(EventMembersAdded, event.DefaultSubscriptionID, m.handleMembersAdded)
	m.eventDispatcher.Unsubscribe(EventMembersAdded, event.MakeSubscriptionID(m.handleInitialMembersAdded))
	close(m.startCh)
}

func (m *ConnectionManager) handleMembersAdded(event event.Event) {
	//if atomic.LoadInt32(&m.state) != ready {
	//	return
	//}
	// do not add new members in non-smart mode
	if !m.smartRouting && m.connMap.Len() > 0 {
		return
	}
	if memberAddedEvent, ok := event.(*MembersAdded); ok {
		missingAddrs := m.connMap.FindAddedAddrs(memberAddedEvent.Members)
		for _, addr := range missingAddrs {
			if err := m.connectAddr(addr); err != nil {
				m.logger.Errorf("error connecting addr: %w", err)
			} else {
				m.logger.Infof("connectionManager member added: %s", addr.String())
			}
		}
	}
}

func (m *ConnectionManager) handleMembersRemoved(event event.Event) {
	if atomic.LoadInt32(&m.state) != ready {
		return
	}
	if memberRemovedEvent, ok := event.(MembersRemoved); ok {
		removedConns := m.connMap.FindRemovedConns(memberRemovedEvent.Members)
		for _, conn := range removedConns {
			m.removeConnection(conn)
		}
	}
}

func (m *ConnectionManager) handleConnectionClosed(event event.Event) {
	if atomic.LoadInt32(&m.state) != ready {
		return
	}
	if connectionClosedEvent, ok := event.(*ConnectionClosed); ok {
		respawnConnection := false
		if err := connectionClosedEvent.Err; err != nil {
			respawnConnection = true
		}
		conn := connectionClosedEvent.Conn
		m.removeConnection(conn)
		if respawnConnection {
			if addr := m.connMap.GetAddrForConnectionID(conn.connectionID); addr != nil {
				if err := m.connectAddr(addr); err != nil {
					m.logger.Errorf("error connecting addr: %w", err)
					// TODO: add failing addrs to a channel
				}
			}
		}
	}
}

func (m *ConnectionManager) removeConnection(conn *Connection) {
	if remaining := m.connMap.RemoveConnection(conn); remaining == 0 {
		m.eventDispatcher.Publish(NewDisconnected())
	}
}

func (m *ConnectionManager) connectCluster() error {
	candidateAddrs := m.clusterService.memberCandidateAddrs()
	if len(candidateAddrs) == 0 {
		return cb.WrapNonRetryableError(errors.New("no member candidate addresses"))
	}
	for _, addr := range candidateAddrs {
		if err := m.connectAddr(addr); err != nil {
			m.logger.Errorf("cannot connect to %s: %w", addr.String(), err)
		} else {
			return nil
		}
	}
	return errors.New("cannot connect to any address in the cluster")
}

func (m *ConnectionManager) connectAddr(addr *pubcluster.AddressImpl) error {
	_, err := m.ensureConnection(addr)
	return err
}

func (m *ConnectionManager) ensureConnection(addr *pubcluster.AddressImpl) (*Connection, error) {
	if conn := m.getConnection(addr); conn != nil {
		return conn, nil
	}
	addr = m.addressTranslator.Translate(addr)
	return m.maybeCreateConnection(addr)
}

func (m *ConnectionManager) getConnection(addr *pubcluster.AddressImpl) *Connection {
	return m.GetConnectionForAddress(addr)
}

func (m *ConnectionManager) maybeCreateConnection(addr pubcluster.Address) (*Connection, error) {
	// TODO: check whether we can create a connection
	conn := m.createDefaultConnection()
	if err := conn.start(m.clusterConfig, addr); err != nil {
		return nil, hzerrors.NewHazelcastTargetDisconnectedError(err.Error(), err)
	} else if err = m.authenticate(conn); err != nil {
		conn.close(nil)
		return nil, err
	}
	return conn, nil
}

func (m *ConnectionManager) createDefaultConnection() *Connection {
	return &Connection{
		responseCh:      m.responseCh,
		pending:         make(chan *proto.ClientMessage, 1024),
		doneCh:          make(chan struct{}),
		writeBuffer:     make([]byte, bufferSize),
		connectionID:    m.NextConnectionID(),
		eventDispatcher: m.eventDispatcher,
		status:          0,
		logger:          m.logger,
		clusterConfig:   m.clusterConfig,
	}
}

func (m *ConnectionManager) authenticate(connection *Connection) error {
	m.credentials.SetEndpoint(connection.socket.LocalAddr().String())
	request := m.encodeAuthenticationRequest()
	inv := m.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, connection, nil)
	select {
	case m.requestCh <- inv:
		if result, err := inv.Get(); err != nil {
			return err
		} else {
			return m.processAuthenticationResult(connection, result)
		}
	case <-m.doneCh:
		return errors.New("done")
	}
}

func (m *ConnectionManager) processAuthenticationResult(conn *Connection, result *proto.ClientMessage) error {
	// TODO: use memberUUID v
	status, address, _, _, serverHazelcastVersion, partitionCount, _, _ := codec.DecodeClientAuthenticationResponse(result)
	switch status {
	case authenticated:
		conn.setConnectedServerVersion(serverHazelcastVersion)
		addrImpl := address.(*pubcluster.AddressImpl)
		conn.endpoint.Store(addrImpl)
		m.connMap.AddConnection(conn, addrImpl)
		// TODO: detect cluster change
		if err := m.partitionService.checkAndSetPartitionCount(partitionCount); err != nil {
			return err
		}
		m.logger.Infof("opened connection to: %s", address)
		m.eventDispatcher.Publish(NewConnectionOpened(conn))
	case credentialsFailed:
		return hzerrors.NewHazelcastAuthenticationError("invalid credentials", nil)
	case serializationVersionMismatch:
		return hzerrors.NewHazelcastAuthenticationError("serialization version mismatches with the server", nil)
	}
	return nil
}

func (m *ConnectionManager) encodeAuthenticationRequest() *proto.ClientMessage {
	if creds, ok := m.credentials.(*security.UsernamePasswordCredentials); ok {
		return m.createAuthenticationRequest(creds)
	}
	panic("only username password credentials are supported")

}

func (m *ConnectionManager) createAuthenticationRequest(creds *security.UsernamePasswordCredentials) *proto.ClientMessage {
	return codec.EncodeClientAuthenticationRequest(
		m.clusterConfig.Name,
		creds.Username(),
		creds.Password(),
		m.clientUUID,
		proto.ClientType,
		byte(serializationVersion),
		ClientVersion,
		m.clientName,
		nil,
	)
}

func (m *ConnectionManager) heartbeat() {
	ticker := time.NewTicker(m.clusterConfig.HeartbeatInterval)
	for {
		select {
		case <-m.doneCh:
			ticker.Stop()
			return
		case <-ticker.C:
			for _, conn := range m.ActiveConnections() {
				m.sendHeartbeat(conn)
			}
		}
	}
}

func (m *ConnectionManager) sendHeartbeat(conn *Connection) {
	request := codec.EncodeClientPingRequest()
	inv := m.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, conn, nil)
	m.requestCh <- inv
}

func (m *ConnectionManager) logStatus() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-m.doneCh:
			ticker.Stop()
			return
		case <-ticker.C:
			m.connMap.Info(func(connections map[string]*Connection, connToAddr map[int64]*pubcluster.AddressImpl) {
				m.logger.Debug(func() string {
					conns := map[string]int{}
					for addr, conn := range connections {
						conns[addr] = int(conn.connectionID)
					}
					return fmt.Sprintf("addrToConn: %#v", conns)
				})
				m.logger.Debug(func() string {
					ctas := map[int]string{}
					for connID, addr := range connToAddr {
						ctas[int(connID)] = addr.String()
					}
					return fmt.Sprintf("connection to address: %#v", ctas)
				})
			})
		}
	}
}

func (m *ConnectionManager) detectFixBrokenConnections() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-m.doneCh:
			ticker.Stop()
			return
		case <-ticker.C:
			// TODO: very inefficent, fix this
			// find and connect the first connection which exists in the cluster but not in the connection manager
			for _, addrStr := range m.clusterService.MemberAddrs() {
				if conn := m.connMap.GetConnectionForAddr(addrStr); conn == nil {
					m.logger.Infof("found a broken connection to: %s, trying to fix it.", addrStr)
					if addr, err := ParseAddress(addrStr); err != nil {
						m.logger.Warnf("cannot parse address: %s", addrStr)
					} else if err := m.connectAddr(addr); err != nil {
						m.logger.Debug(func() string {
							return fmt.Sprintf("cannot fix connection to %s: %s", addr, err.Error())
						})
					}
				}
			}
			if m.connMap.Len() == 0 {
				// if there are no connections, try to connect to seeds
				if err := m.connectCluster(); err != nil {
					m.logger.Errorf("while trying to fix cluster connection: %w", err)
				}
			}
		}
	}
}

type connectionMap struct {
	connectionsMu *sync.RWMutex
	// addrToConn maps connection address to connection
	addrToConn map[string]*Connection
	// connToAddr maps connection ID to address
	connToAddr map[int64]*pubcluster.AddressImpl
}

func newConnectionMap() *connectionMap {
	return &connectionMap{
		connectionsMu: &sync.RWMutex{},
		addrToConn:    map[string]*Connection{},
		connToAddr:    map[int64]*pubcluster.AddressImpl{},
	}
}

func (m *connectionMap) AddConnection(conn *Connection, addr *pubcluster.AddressImpl) {
	m.connectionsMu.Lock()
	defer m.connectionsMu.Unlock()
	m.addrToConn[addr.String()] = conn
	m.connToAddr[conn.connectionID] = addr
}

// RemoveConnection removes a connection and returns true if there are no more addrToConn left.
func (m *connectionMap) RemoveConnection(removedConn *Connection) int {
	m.connectionsMu.Lock()
	defer m.connectionsMu.Unlock()
	var remaining int
	for addr, conn := range m.addrToConn {
		if conn.connectionID == removedConn.connectionID {
			delete(m.addrToConn, addr)
			delete(m.connToAddr, conn.connectionID)
			remaining = len(m.addrToConn)
			break
		}
	}
	return remaining
}

func (m *connectionMap) CloseAll() {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	for _, conn := range m.addrToConn {
		conn.close(nil)
	}
}

func (m *connectionMap) GetConnectionForAddr(addr string) *Connection {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	if conn, ok := m.addrToConn[addr]; ok {
		return conn
	}
	return nil
}

func (m *connectionMap) GetAddrForConnectionID(connID int64) *pubcluster.AddressImpl {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	if addr, ok := m.connToAddr[connID]; ok {
		return addr
	}
	return nil
}

func (m *connectionMap) GetRandomAddr() *pubcluster.AddressImpl {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	for _, addr := range m.connToAddr {
		// get the first address
		return addr
	}
	return nil
}

func (m *connectionMap) RandomConn() *Connection {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	for _, conn := range m.addrToConn {
		// Go randomizes maps, this is random enough for now.
		return conn
	}
	return nil
}

func (m *connectionMap) Connections() []*Connection {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	conns := make([]*Connection, 0, len(m.addrToConn))
	for _, conn := range m.addrToConn {
		conns = append(conns, conn)
	}
	return conns
}

func (m *connectionMap) FindAddedAddrs(members []pubcluster.Member) []*pubcluster.AddressImpl {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	addedAddrs := []*pubcluster.AddressImpl{}
	for _, member := range members {
		addr := member.Address()
		if _, exists := m.addrToConn[addr.String()]; !exists {
			addedAddrs = append(addedAddrs, addr.(*pubcluster.AddressImpl))
		}
	}
	return addedAddrs
}

func (m *connectionMap) FindRemovedConns(members []pubcluster.Member) []*Connection {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	removedConns := []*Connection{}
	for _, member := range members {
		addr := member.Address()
		if conn, exists := m.addrToConn[addr.String()]; exists {
			removedConns = append(removedConns, conn)
		}
	}
	return removedConns
}

func (m *connectionMap) Info(infoFun func(connections map[string]*Connection, connToAddr map[int64]*pubcluster.AddressImpl)) {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	infoFun(m.addrToConn, m.connToAddr)
}

func (m *connectionMap) Len() int {
	m.connectionsMu.RLock()
	l := len(m.connToAddr)
	m.connectionsMu.RUnlock()
	return l
}
