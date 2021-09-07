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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/security"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/util"
	"github.com/hazelcast/hazelcast-go-client/internal/util/nilutil"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	authenticated                = 0
	credentialsFailed            = 1
	serializationVersionMismatch = 2
	notAllowedInCluster          = 3
)

const (
	created int32 = iota
	ready
	stopped
)

const (
	serializationVersion = 1
)

type connectMemberFunc func(ctx context.Context, m *ConnectionManager, addr pubcluster.Address) (pubcluster.Address, error)

func connectMember(ctx context.Context, m *ConnectionManager, addr pubcluster.Address) (pubcluster.Address, error) {
	var initialAddr pubcluster.Address
	var err error
	if _, err = m.ensureConnection(ctx, addr); err != nil {
		m.logger.Errorf("cannot connect to %s: %w", addr.String(), err)
	} else if initialAddr == "" {
		initialAddr = addr
	}
	if initialAddr == "" {
		return "", fmt.Errorf("cannot connect to address in the cluster: %w", err)
	}
	return initialAddr, nil
}

func tryConnectAddress(
	ctx context.Context,
	m *ConnectionManager,
	portRange pubcluster.PortRange,
	addr pubcluster.Address,
	connMember connectMemberFunc,
) (pubcluster.Address, error) {
	host, port, err := internal.ParseAddr(addr.String())
	if err != nil {
		return "", err
	}
	var initialAddr pubcluster.Address
	if port == 0 { // we need to try all addresses in port range
		for _, currAddr := range util.GetAddresses(host, portRange) {
			currentAddrRet, connErr := connMember(ctx, m, currAddr)
			if connErr == nil {
				initialAddr = currentAddrRet
				break
			} else {
				err = connErr
			}
		}
	} else {
		initialAddr, err = connMember(ctx, m, addr)
	}

	if initialAddr == "" {
		return initialAddr, fmt.Errorf("cannot connect to any address in the cluster: %w", err)
	}

	return initialAddr, nil
}

type ConnectionManagerCreationBundle struct {
	Logger               ilogger.Logger
	RequestCh            chan<- invocation.Invocation
	ResponseCh           chan<- *proto.ClientMessage
	PartitionService     *PartitionService
	InvocationFactory    *ConnectionInvocationFactory
	ClusterConfig        *pubcluster.Config
	ClusterService       *Service
	SerializationService *iserialization.Service
	EventDispatcher      *event.DispatchService
	FailoverService      *FailoverService
	FailoverConfig       *pubcluster.FailoverConfig
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
	if b.ClientName == "" {
		panic("ClientName is blank")
	}
	if b.FailoverService == nil {
		panic("FailoverService is nil")
	}
	if b.FailoverConfig == nil {
		panic("FailoverConfig is nil")
	}
}

type ConnectionManager struct {
	logger               ilogger.Logger
	failoverConfig       *pubcluster.FailoverConfig
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
	clusterIDMu          *sync.Mutex
	clusterID            *types.UUID
	requestCh            chan<- invocation.Invocation
	prevClusterID        *types.UUID
	failoverService      *FailoverService
	randGen              *rand.Rand
	clientName           string
	labels               []string
	clientUUID           types.UUID
	nextConnID           int64
	state                int32
	smartRouting         bool
}

func NewConnectionManager(bundle ConnectionManagerCreationBundle) *ConnectionManager {
	bundle.Check()
	manager := &ConnectionManager{
		requestCh:            bundle.RequestCh,
		responseCh:           bundle.ResponseCh,
		clusterService:       bundle.ClusterService,
		partitionService:     bundle.PartitionService,
		serializationService: bundle.SerializationService,
		eventDispatcher:      bundle.EventDispatcher,
		invocationFactory:    bundle.InvocationFactory,
		clusterConfig:        bundle.ClusterConfig,
		clientName:           bundle.ClientName,
		labels:               bundle.Labels,
		clientUUID:           types.NewUUID(),
		connMap:              newConnectionMap(bundle.ClusterConfig.LoadBalancer()),
		smartRouting:         !bundle.ClusterConfig.Unisocket,
		logger:               bundle.Logger,
		failoverService:      bundle.FailoverService,
		failoverConfig:       bundle.FailoverConfig,
		clusterIDMu:          &sync.Mutex{},
		randGen:              rand.New(rand.NewSource(time.Now().Unix())),
	}
	return manager
}

func (m *ConnectionManager) Start(ctx context.Context) error {
	m.reset()
	return m.start(ctx)
}

func (m *ConnectionManager) start(ctx context.Context) error {
	m.logger.Trace(func() string { return "cluster.ConnectionManager.start" })
	m.eventDispatcher.Subscribe(EventMembersAdded, event.DefaultSubscriptionID, m.handleInitialMembersAdded)
	addr, err := m.tryConnectCluster(ctx)
	if err != nil {
		return err
	}
	// wait for initial member list
	m.logger.Debug(func() string { return "cluster.ConnectionManager.start: waiting for the initial member list" })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.startCh:
		break
	}
	m.logger.Debug(func() string { return "cluster.ConnectionManager.start: received the initial member list" })
	// check if cluster ID has changed after reconnection
	var clusterIDChanged bool
	m.clusterIDMu.Lock()
	clusterIDChanged = m.prevClusterID != nil && m.prevClusterID != m.clusterID
	m.clusterIDMu.Unlock()
	if clusterIDChanged {
		m.eventDispatcher.Publish(NewChangedCluster())
	}
	m.eventDispatcher.Publish(NewConnected(addr))
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
	return nil
}

func (m *ConnectionManager) Stop() {
	m.eventDispatcher.Unsubscribe(EventConnectionClosed, event.MakeSubscriptionID(m.handleConnectionClosed))
	m.eventDispatcher.Unsubscribe(EventMembersAdded, event.MakeSubscriptionID(m.handleMembersAdded))
	m.eventDispatcher.Unsubscribe(EventMembersRemoved, event.MakeSubscriptionID(m.handleMembersRemoved))
	if !atomic.CompareAndSwapInt32(&m.state, ready, stopped) {
		return
	}
	close(m.doneCh)
	m.connMap.CloseAll(nil)
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

func (m *ConnectionManager) reset() {
	m.doneCh = make(chan struct{}, 1)
	m.startCh = make(chan struct{}, 1)
	m.clusterIDMu.Lock()
	if m.clusterID != nil {
		m.prevClusterID = m.clusterID
	}
	m.clusterID = nil
	m.clusterIDMu.Unlock()
	m.connMap.Reset()
}

func (m *ConnectionManager) handleInitialMembersAdded(e event.Event) {
	m.logger.Trace(func() string { return "connectionManager.handleInitialMembersAdded" })
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
		m.logger.Trace(func() string {
			return fmt.Sprintf("connectionManager.handleMembersAdded: %v", e.Members)
		})
		missingAddrs := m.connMap.FindAddedAddrs(e.Members, m.clusterService)
		for _, addr := range missingAddrs {
			if _, err := connectMember(context.TODO(), m, addr); err != nil {
				m.logger.Errorf("connecting address: %w", err)
			} else {
				m.logger.Infof("connectionManager member added: %s", addr.String())
			}
		}
		return
	}
	m.logger.Warnf("connectionManager.handleMembersAdded: expected *MembersAdded event")
}

func (m *ConnectionManager) handleMembersRemoved(event event.Event) {
	if e, ok := event.(*MembersRemoved); ok {
		m.logger.Trace(func() string {
			return fmt.Sprintf("connectionManager.handleMembersRemoved: %v", e.Members)
		})
		removedConns := m.connMap.FindRemovedConns(e.Members)
		for _, conn := range removedConns {
			m.removeConnection(conn)
		}
		return
	}
	m.logger.Warnf("connectionManager.handleMembersRemoved: expected *MembersRemoved event")
}

func (m *ConnectionManager) handleConnectionClosed(event event.Event) {
	if atomic.LoadInt32(&m.state) != ready {
		return
	}
	e, ok := event.(*ConnectionClosed)
	if !ok {
		return
	}
	conn := e.Conn
	m.removeConnection(conn)
	if m.connMap.Len() == 0 {
		m.logger.Debug(func() string { return "ConnectionManager.handleConnectionClosed: no connections left" })
		return
	}
	var respawnConnection bool
	if err := e.Err; err != nil {
		m.logger.Debug(func() string { return fmt.Sprintf("respawning connection, since: %s", err.Error()) })
		respawnConnection = true
	} else {
		m.logger.Debug(func() string { return "not respawning connection, no errors" })
	}
	if respawnConnection {
		if addr, ok := m.connMap.GetAddrForConnectionID(conn.connectionID); ok {
			if _, err := m.ensureConnection(context.TODO(), addr); err != nil {
				m.logger.Errorf("error connecting address: %w", err)
				// TODO: add failing addrs to a channel
			}
		}
	}
}

func (m *ConnectionManager) removeConnection(conn *Connection) {
	if remaining := m.connMap.RemoveConnection(conn); remaining == 0 {
		m.eventDispatcher.Publish(NewDisconnected())
	}
}

func (m *ConnectionManager) tryConnectCluster(ctx context.Context) (pubcluster.Address, error) {
	tryCount := 1
	if m.failoverConfig.Enabled {
		tryCount = m.failoverConfig.TryCount
	}
	for i := 1; i <= tryCount; i++ {
		cluster := m.failoverService.Current()
		m.logger.Infof("trying to connect to cluster: %s", cluster.ClusterName)
		addr, err := m.tryConnectCandidateCluster(ctx, cluster, cluster.ConnectionStrategy)
		if err == nil {
			m.logger.Infof("connected to cluster: %s", m.failoverService.Current().ClusterName)
			return addr, nil
		}
		if nonRetryableConnectionErr(err) {
			break
		}
		m.failoverService.Next()
	}
	return "", fmt.Errorf("cannot connect to any cluster: %w", hzerrors.ErrIllegalState)
}

func (m *ConnectionManager) tryConnectCandidateCluster(ctx context.Context, cluster *CandidateCluster, cs *pubcluster.ConnectionStrategyConfig) (pubcluster.Address, error) {
	cbr := cb.NewCircuitBreaker(
		cb.MaxRetries(math.MaxInt32),
		cb.Timeout(time.Duration(cs.Timeout)),
		cb.MaxFailureCount(3),
		cb.RetryPolicy(makeRetryPolicy(m.randGen, &cs.Retry)),
	)
	addr, err := cbr.TryContext(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		addr, err := m.connectCluster(ctx, cluster)
		if err != nil {
			m.logger.Debug(func() string {
				return fmt.Sprintf("connectionManager: error connecting to cluster, attempt %d: %s", attempt+1, err.Error())
			})
		}
		return addr, err
	})
	if err != nil {
		return "", err
	}
	return addr.(pubcluster.Address), nil
}

func (m *ConnectionManager) connectCluster(ctx context.Context, cluster *CandidateCluster) (pubcluster.Address, error) {
	seedAddrs, err := m.clusterService.RefreshedSeedAddrs(cluster)
	if err != nil {
		return "", fmt.Errorf("failed to refresh seed addresses: %w", err)
	}
	if len(seedAddrs) == 0 {
		return "", errors.New("could not find any seed addresses")
	}
	var initialAddr pubcluster.Address
	for _, addr := range seedAddrs {
		initialAddr, err = tryConnectAddress(ctx, m, m.clusterConfig.Network.PortRange, addr, connectMember)
		if err != nil {
			return "", err
		}
	}
	if initialAddr == "" {
		return "", err
	}
	return initialAddr, nil
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
		return nil, ihzerrors.NewTargetDisconnectedError(err.Error(), err)
	} else if err = m.authenticate(ctx, conn); err != nil {
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

func (m *ConnectionManager) authenticate(ctx context.Context, conn *Connection) error {
	cluster := m.failoverService.Current()
	m.logger.Debug(func() string {
		return fmt.Sprintf("authenticate: cluster name: %s; local: %s; remote: %s; addr: %s",
			cluster.ClusterName, conn.socket.LocalAddr(), conn.socket.RemoteAddr(), conn.Endpoint())
	})
	credentials := cluster.Credentials
	credentials.SetEndpoint(conn.LocalAddr())
	request := m.encodeAuthenticationRequest()
	inv := m.invocationFactory.NewConnectionBoundInvocation(request, conn, nil)
	m.logger.Debug(func() string {
		return fmt.Sprintf("authentication correlation ID: %d", inv.Request().CorrelationID())
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.requestCh <- inv:
		if result, err := inv.GetWithContext(ctx); err != nil {
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
	status, address, _, _, serverHazelcastVersion, partitionCount, newClusterID, failoverSupported := codec.DecodeClientAuthenticationResponse(result)
	if m.failoverConfig.Enabled && !failoverSupported {
		m.logger.Warnf("cluster does not support failover: this feature is available in Hazelcast Enterprise")
		status = notAllowedInCluster
	}
	switch status {
	case authenticated:
		conn.setConnectedServerVersion(serverHazelcastVersion)
		connAddr, err := m.failoverService.Current().AddressTranslator.Translate(context.TODO(), *address)
		if err != nil {
			return err
		}
		if err := m.partitionService.checkAndSetPartitionCount(partitionCount); err != nil {
			return err
		}

		m.logger.Trace(func() string {
			return fmt.Sprintf("connectionManager: checking the cluster: %v, current cluster: %v", newClusterID, m.clusterID)
		})
		m.clusterIDMu.Lock()
		// clusterID is nil only at the start of the client,
		// or at the start of a reconnection attempt.
		// It is only set in this method below under failoverMu.
		// clusterID is set by master when a cluster is started.
		// clusterID is not preserved during HotRestart.
		// In split brain, both sides have the same clusterID
		clusterIDChanged := m.clusterID != nil && *m.clusterID != newClusterID
		if clusterIDChanged {
			// If the cluster ID has changed that means we have a connection to wrong cluster.
			// It could also mean a cluster restart.
			// We should not stay connected to this new connection, so we disconnect.
			// In the restart scenario, we just force the disconnect event handler to trigger a reconnection.
			conn.close(nil)
			m.clusterIDMu.Unlock()
			return fmt.Errorf("connection does not belong to this cluster: %w", hzerrors.ErrIllegalState)
		}
		if m.connMap.IsEmpty() {
			// the first connection that opens a connection to the new cluster should set clusterID
			m.clusterID = &newClusterID
		}
		m.connMap.AddConnection(conn, connAddr)
		m.clusterIDMu.Unlock()
		m.logger.Debug(func() string {
			return fmt.Sprintf("opened connection to: %s", connAddr)
		})
		m.eventDispatcher.Publish(NewConnectionOpened(conn))
		return nil
	case credentialsFailed:
		return cb.WrapNonRetryableError(fmt.Errorf("invalid credentials: %w", hzerrors.ErrAuthentication))
	case serializationVersionMismatch:
		return cb.WrapNonRetryableError(fmt.Errorf("serialization version mismatches with the server: %w", hzerrors.ErrAuthentication))
	case notAllowedInCluster:
		return cb.WrapNonRetryableError(hzerrors.ErrClientNotAllowedInCluster)
	}
	return hzerrors.ErrAuthentication
}

func (m *ConnectionManager) encodeAuthenticationRequest() *proto.ClientMessage {
	clusterName := m.failoverService.Current().ClusterName
	credentials := m.failoverService.Current().Credentials
	if creds, ok := credentials.(*security.UsernamePasswordCredentials); ok {
		return m.createAuthenticationRequest(clusterName, creds)
	}
	panic("only username password credentials are supported")
}

func (m *ConnectionManager) createAuthenticationRequest(clusterName string, creds *security.UsernamePasswordCredentials) *proto.ClientMessage {
	return codec.EncodeClientAuthenticationRequest(
		clusterName,
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
	ticker := time.NewTicker(time.Duration(m.clusterConfig.HeartbeatInterval))
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
	select {
	case m.requestCh <- inv:
	case <-time.After(time.Duration(m.clusterConfig.HeartbeatInterval)):
		return
	}
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
	defer ticker.Stop()
	for {
		select {
		case <-m.doneCh:
			return
		case <-ticker.C:
			// TODO: very inefficent, fix this
			// find connections which exist in the cluster but not in the connection manager
			for _, addr := range m.clusterService.MemberAddrs() {
				m.checkFixConnection(addr)
			}
		}
	}
}

func (m *ConnectionManager) checkFixConnection(addr pubcluster.Address) {
	if conn := m.connMap.GetConnectionForAddr(addr); conn == nil {
		m.logger.Debug(func() string {
			return fmt.Sprintf("found a broken connection to: %s, trying to fix it.", addr)
		})
		ctx := context.Background()
		if _, err := connectMember(ctx, m, addr); err != nil {
			m.logger.Debug(func() string {
				return fmt.Sprintf("cannot fix connection to %s: %s", addr, err.Error())
			})
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

func (m *connectionMap) Reset() {
	m.mu.Lock()
	m.addrToConn = map[pubcluster.Address]*Connection{}
	m.connToAddr = map[int64]pubcluster.Address{}
	m.mu.Unlock()
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

func (m *connectionMap) CloseAll(err error) {
	m.mu.RLock()
	for _, conn := range m.addrToConn {
		conn.close(err)
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

func (m *connectionMap) IsEmpty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.connToAddr) == 0
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

func nonRetryableConnectionErr(err error) bool {
	var ne cb.NonRetryableError
	return ne.Is(err) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
