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
	"github.com/hazelcast/hazelcast-go-client/internal"
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

const (
	serializationVersion = 1
)

type ConnectionManagerCreationBundle struct {
	Logger               ilogger.Logger
	Credentials          security.Credentials
	AddrTranslator       AddressTranslator
	RequestCh            chan<- invocation.Invocation
	ResponseCh           chan<- *proto.ClientMessage
	PartitionService     *PartitionService
	InvocationFactory    *ConnectionInvocationFactory
	ClusterConfig        *pubcluster.Config
	ClusterService       *Service
	SerializationService *iserialization.Service
	EventDispatcher      *event.DispatchService
	ClientName           string
	Labels               []string
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
	if b.AddrTranslator == nil {
		panic("AddrTranslator is nil")
	}
}

type ConnectionManager struct {
	logger               ilogger.Logger
	credentials          security.Credentials
	cb                   *cb.CircuitBreaker
	partitionService     *PartitionService
	serializationService *iserialization.Service
	eventDispatcher      *event.DispatchService
	invocationFactory    *ConnectionInvocationFactory
	clusterService       *Service
	responseCh           chan<- *proto.ClientMessage
	startCh              chan struct{}
	connMap              *connectionMap
	doneCh               chan struct{}
	clusterConfig        *pubcluster.Config
	requestCh            chan<- invocation.Invocation
	addrTranslator       AddressTranslator
	clientName           string
	labels               []string
	clientUUID           types.UUID
	nextConnID           int64
	state                int32
	smartRouting         bool
}

func NewConnectionManager(bundle ConnectionManagerCreationBundle) *ConnectionManager {
	bundle.Check()
	// TODO: make circuit breaker configurable
	cbr := cb.NewCircuitBreaker(
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
	lb := bundle.ClusterConfig.LoadBalancer()
	if lb == nil {
		lb = pubcluster.NewRoundRobinLoadBalancer()
	}
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
		labels:               bundle.Labels,
		clientUUID:           types.NewUUID(),
		connMap:              newConnectionMap(lb),
		smartRouting:         bundle.ClusterConfig.SmartRouting,
		logger:               bundle.Logger,
		doneCh:               make(chan struct{}, 1),
		startCh:              make(chan struct{}, 1),
		cb:                   cbr,
		addrTranslator:       bundle.AddrTranslator,
	}
	return manager
}

func (m *ConnectionManager) Start(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&m.state, created, starting) {
		return nil
	}
	return m.start(context.TODO(), timeout)
}

func (m *ConnectionManager) start(ctx context.Context, timeout time.Duration) error {
	m.eventDispatcher.Subscribe(EventMembersAdded, event.DefaultSubscriptionID, m.handleInitialMembersAdded)
	addr, err := m.cb.TryContext(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		addr, err := m.connectCluster(ctx, false)
		if err != nil {
			m.logger.Errorf("error starting: %w", err)
		}
		return addr, err
	})
	if err != nil {
		return err
	}
	// wait for the initial member list
	select {
	case <-m.startCh:
		break
	case <-time.After(timeout):
		return errors.New("initial member list not received in deadline")
	}
	go m.heartbeat()
	if m.smartRouting {
		// fix broken connections only in the smart mode
		go m.detectFixBrokenConnections()
	}
	if m.logger.CanLogDebug() {
		go m.logStatus()
	}
	atomic.StoreInt32(&m.state, ready)
	m.eventDispatcher.Subscribe(EventConnectionClosed, event.DefaultSubscriptionID, m.handleConnectionClosed)
	m.eventDispatcher.Publish(NewConnected(addr.(pubcluster.Address)))
	return nil
}

func (m *ConnectionManager) Stop() {
	if !atomic.CompareAndSwapInt32(&m.state, ready, stopping) {
		close(m.doneCh)
		return
	}
	m.eventDispatcher.Unsubscribe(EventConnectionClosed, event.MakeSubscriptionID(m.handleConnectionClosed))
	m.eventDispatcher.Unsubscribe(EventMembersAdded, event.MakeSubscriptionID(m.handleMembersAdded))
	m.eventDispatcher.Unsubscribe(EventMembersRemoved, event.MakeSubscriptionID(m.handleMembersRemoved))
	m.connMap.CloseAll()
	atomic.StoreInt32(&m.state, stopped)
	close(m.doneCh)
}

func (m *ConnectionManager) NextConnectionID() int64 {
	return atomic.AddInt64(&m.nextConnID, 1)
}

func (m *ConnectionManager) GetConnectionForAddress(addr pubcluster.Address) *Connection {
	return m.connMap.GetConnectionForAddr(addr)
}

func (m *ConnectionManager) GetConnectionForPartition(partitionID int32) *Connection {
	if partitionID < 0 {
		panic("partition ID is negative")
	}
	if ownerUUID, ok := m.partitionService.GetPartitionOwner(partitionID); !ok {
		return nil
	} else if member := m.clusterService.GetMemberByUUID(ownerUUID); nilutil.IsNil(member) {
		return nil
	} else {
		return m.GetConnectionForAddress(member.Address)
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
	m.eventDispatcher.Subscribe(EventMembersRemoved, event.DefaultSubscriptionID, m.handleMembersRemoved)
	m.eventDispatcher.Unsubscribe(EventMembersAdded, event.MakeSubscriptionID(m.handleInitialMembersAdded))
	close(m.startCh)
}

func (m *ConnectionManager) handleMembersAdded(event event.Event) {
	// do not add new members in non-smart mode
	if !m.smartRouting && m.connMap.Len() > 0 {
		return
	}
	if e, ok := event.(*MembersAdded); ok {
		missingAddrs := m.connMap.FindAddedAddrs(e.Members, m.clusterService)
		for _, addr := range missingAddrs {
			if _, err := m.ensureConnection(context.TODO(), addr); err != nil {
				m.logger.Errorf("connecting addr: %w", err)
			} else {
				m.logger.Infof("connectionManager member added: %s", addr.String())
			}
		}
	}
}

func (m *ConnectionManager) handleMembersRemoved(event event.Event) {
	if e, ok := event.(MembersRemoved); ok {
		removedConns := m.connMap.FindRemovedConns(e.Members)
		for _, conn := range removedConns {
			m.removeConnection(conn)
		}
	}
}

func (m *ConnectionManager) handleConnectionClosed(event event.Event) {
	if atomic.LoadInt32(&m.state) != ready {
		return
	}
	if e, ok := event.(*ConnectionClosed); ok {
		respawnConnection := false
		if err := e.Err; err != nil {
			respawnConnection = true
		}
		conn := e.Conn
		m.removeConnection(conn)
		if respawnConnection {
			if addr, ok := m.connMap.GetAddrForConnectionID(conn.connectionID); ok {
				if _, err := m.ensureConnection(context.TODO(), addr); err != nil {
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

func (m *ConnectionManager) connectCluster(ctx context.Context, refresh bool) (pubcluster.Address, error) {
	seedAddrs := m.clusterService.RefreshedSeedAddrs(refresh)
	if len(seedAddrs) == 0 {
		return "", cb.WrapNonRetryableError(errors.New("no seed addresses"))
	}
	for _, addr := range seedAddrs {
		if conn, err := m.ensureConnection(ctx, addr); err != nil {
			m.logger.Errorf("cannot connect to %s: %w", addr.String(), err)
		} else if err := m.clusterService.sendMemberListViewRequest(ctx, conn); err != nil {
			return "", err
		} else {
			return addr, nil
		}
	}
	return "", errors.New("cannot connect to any address in the cluster")
}

func (m *ConnectionManager) ensureConnection(ctx context.Context, addr pubcluster.Address) (*Connection, error) {
	if conn := m.getConnection(addr); conn != nil {
		return conn, nil
	}
	return m.maybeCreateConnection(ctx, addr)
}

func (m *ConnectionManager) getConnection(addr pubcluster.Address) *Connection {
	return m.GetConnectionForAddress(addr)
}

func (m *ConnectionManager) maybeCreateConnection(ctx context.Context, addr pubcluster.Address) (*Connection, error) {
	// TODO: check whether we can create a connection
	conn := m.createDefaultConnection(addr)
	if err := conn.start(m.clusterConfig, addr); err != nil {
		return nil, hzerrors.NewHazelcastTargetDisconnectedError(err.Error(), err)
	} else if err = m.authenticate(conn); err != nil {
		conn.close(nil)
		return nil, err
	}
	return conn, nil
}

func (m *ConnectionManager) createDefaultConnection(addr pubcluster.Address) *Connection {
	conn := &Connection{
		responseCh:      m.responseCh,
		pending:         make(chan *proto.ClientMessage, 1024),
		doneCh:          make(chan struct{}),
		connectionID:    m.NextConnectionID(),
		eventDispatcher: m.eventDispatcher,
		status:          0,
		logger:          m.logger,
		clusterConfig:   m.clusterConfig,
	}
	conn.endpoint.Store(addr)
	return conn
}

func (m *ConnectionManager) authenticate(conn *Connection) error {
	m.logger.Trace(func() string {
		return fmt.Sprintf("authenticate: local: %s; remote: %s; addr: %s",
			conn.socket.LocalAddr(), conn.socket.RemoteAddr(), conn.Endpoint())
	})
	m.credentials.SetEndpoint(conn.LocalAddr())
	request := m.encodeAuthenticationRequest()
	inv := m.invocationFactory.NewConnectionBoundInvocation(request, conn, nil)
	select {
	case m.requestCh <- inv:
		if result, err := inv.GetWithContext(context.TODO()); err != nil {
			return err
		} else {
			return m.processAuthenticationResult(conn, result)
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
		connAddr, err := m.addrTranslator.Translate(context.TODO(), *address)
		if err != nil {
			return err
		}
		m.connMap.AddConnection(conn, connAddr)
		// TODO: detect cluster change
		if err := m.partitionService.checkAndSetPartitionCount(partitionCount); err != nil {
			return err
		}
		m.logger.Debug(func() string {
			return fmt.Sprintf("opened connection to: %s", connAddr)
		})
		m.eventDispatcher.Publish(NewConnectionOpened(conn))
		return nil
	case credentialsFailed:
		return fmt.Errorf("invalid credentials: %w", hzerrors.ErrAuthentication)
	case serializationVersionMismatch:
		return fmt.Errorf("serialization version mismatches with the server: %w", hzerrors.ErrAuthentication)
	}
	return hzerrors.ErrAuthentication
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
		internal.ClientType,
		byte(serializationVersion),
		internal.ClientVersion,
		m.clientName,
		m.labels,
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
	inv := m.invocationFactory.NewConnectionBoundInvocation(request, conn, nil)
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
			m.connMap.Info(func(connections map[pubcluster.Address]*Connection, connToAddr map[int64]pubcluster.Address) {
				m.logger.Debug(func() string {
					conns := map[pubcluster.Address]int{}
					for addr, conn := range connections {
						conns[addr] = int(conn.connectionID)
					}
					return fmt.Sprintf("address to connection: %+v", conns)
				})
				m.logger.Debug(func() string {
					return fmt.Sprintf("connection to address: %+v", connToAddr)
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
			// find connections which exists in the cluster but not in the connection manager
			for _, addr := range m.clusterService.MemberAddrs() {
				if conn := m.connMap.GetConnectionForAddr(addr); conn == nil {
					m.logger.Infof("found a broken connection to: %s, trying to fix it.", addr)
					if _, err := m.ensureConnection(context.TODO(), addr); err != nil {
						m.logger.Debug(func() string {
							return fmt.Sprintf("cannot fix connection to %s: %s", addr, err.Error())
						})
					}
				}
			}
			if m.connMap.Len() == 0 {
				// if there are no connections, try to connect to seeds
				if _, err := m.connectCluster(context.TODO(), true); err != nil {
					m.logger.Errorf("while trying to fix cluster connection: %w", err)
				}
			}
		}
	}
}

type connectionMap struct {
	lb pubcluster.LoadBalancer
	mu *sync.RWMutex
	// addrToConn maps connection address to connection
	addrToConn map[pubcluster.Address]*Connection
	// connToAddr maps connection ID to address
	connToAddr map[int64]pubcluster.Address
	addrs      []pubcluster.Address
}

func newConnectionMap(lb pubcluster.LoadBalancer) *connectionMap {
	return &connectionMap{
		lb:         lb,
		mu:         &sync.RWMutex{},
		addrToConn: map[pubcluster.Address]*Connection{},
		connToAddr: map[int64]pubcluster.Address{},
	}
}

func (m *connectionMap) AddConnection(conn *Connection, addr pubcluster.Address) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// if the connection was already added, skip it
	if _, ok := m.connToAddr[conn.connectionID]; ok {
		return
	}
	m.addrToConn[addr] = conn
	m.connToAddr[conn.connectionID] = addr
	m.addrs = append(m.addrs, addr)
}

// RemoveConnection removes a connection and returns the number of remaining connections.
func (m *connectionMap) RemoveConnection(removedConn *Connection) int {
	m.mu.Lock()
	var remaining int
	for addr, conn := range m.addrToConn {
		if conn.connectionID == removedConn.connectionID {
			delete(m.addrToConn, addr)
			delete(m.connToAddr, conn.connectionID)
			m.removeAddr(addr)
			break
		}
	}
	remaining = len(m.addrToConn)
	m.mu.Unlock()
	return remaining
}

func (m *connectionMap) CloseAll() {
	m.mu.RLock()
	for _, conn := range m.addrToConn {
		conn.close(nil)
	}
	m.mu.RUnlock()
}

func (m *connectionMap) GetConnectionForAddr(addr pubcluster.Address) *Connection {
	m.mu.RLock()
	conn := m.addrToConn[addr]
	m.mu.RUnlock()
	return conn
}

func (m *connectionMap) GetAddrForConnectionID(connID int64) (pubcluster.Address, bool) {
	m.mu.RLock()
	addr, ok := m.connToAddr[connID]
	m.mu.RUnlock()
	return addr, ok
}

func (m *connectionMap) RandomConn() *Connection {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.addrs) == 0 {
		return nil
	}
	var addr pubcluster.Address
	if len(m.addrs) == 1 {
		addr = m.addrs[0]
	} else {
		addr = m.lb.OneOf(m.addrs)
	}
	conn := m.addrToConn[addr]
	if conn != nil {
		return conn
	}
	// if the connection was not found by using the load balancer, select a random one.
	for _, conn = range m.addrToConn {
		// Go randomizes maps, this is random enough for now.
		return conn
	}
	return nil
}

func (m *connectionMap) Connections() []*Connection {
	m.mu.RLock()
	conns := make([]*Connection, 0, len(m.addrToConn))
	for _, conn := range m.addrToConn {
		conns = append(conns, conn)
	}
	m.mu.RUnlock()
	return conns
}

func (m *connectionMap) FindAddedAddrs(members []pubcluster.MemberInfo, cs *Service) []pubcluster.Address {
	m.mu.RLock()
	addedAddrs := make([]pubcluster.Address, 0, len(members))
	for _, member := range members {
		addr, err := cs.MemberAddr(&member)
		if err != nil {
			continue
		}
		if _, exists := m.addrToConn[addr]; !exists {
			addedAddrs = append(addedAddrs, addr)
		}
	}
	m.mu.RUnlock()
	return addedAddrs
}

func (m *connectionMap) FindRemovedConns(members []pubcluster.MemberInfo) []*Connection {
	m.mu.RLock()
	removedConns := []*Connection{}
	for _, member := range members {
		addr := member.Address
		if conn, exists := m.addrToConn[addr]; exists {
			removedConns = append(removedConns, conn)
		}
	}
	m.mu.RUnlock()
	return removedConns
}

func (m *connectionMap) Info(infoFun func(connections map[pubcluster.Address]*Connection, connToAddr map[int64]pubcluster.Address)) {
	m.mu.RLock()
	infoFun(m.addrToConn, m.connToAddr)
	m.mu.RUnlock()
}

func (m *connectionMap) Len() int {
	m.mu.RLock()
	l := len(m.connToAddr)
	m.mu.RUnlock()
	return l
}

func (m *connectionMap) removeAddr(addr pubcluster.Address) {
	for i, a := range m.addrs {
		if a.Equal(addr) {
			// note that this changes the order of addresses
			m.addrs[i] = m.addrs[len(m.addrs)-1]
			m.addrs = m.addrs[:len(m.addrs)-1]
			break
		}
	}
}
