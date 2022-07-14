/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/internal/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/security"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	authenticated                = 0
	credentialsFailed            = 1
	serializationVersionMismatch = 2
	notAllowedInCluster          = 3
)

const (
	starting int32 = iota
	ready
	stopped
)

const (
	serializationVersion  = 1
	initialMembersTimeout = 120 * time.Second
)

var connectionManagerSubID = event.NextSubscriptionID()

type connectMemberFunc func(ctx context.Context, m *ConnectionManager, addr pubcluster.Address, networkCfg *pubcluster.NetworkConfig) (pubcluster.Address, error)

type ConnectionManagerCreationBundle struct {
	Logger               logger.LogAdaptor
	PartitionService     *PartitionService
	InvocationFactory    *ConnectionInvocationFactory
	ClusterConfig        *pubcluster.Config
	ClusterService       *Service
	SerializationService *iserialization.Service
	EventDispatcher      *event.DispatchService
	FailoverService      *FailoverService
	FailoverConfig       *pubcluster.FailoverConfig
	IsClientShutdown     func() bool
	ClientName           string
	Labels               []string
}

func (b ConnectionManagerCreationBundle) Check() {
	if b.Logger.Logger == nil {
		panic("LogAdaptor is nil")
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
	if b.IsClientShutdown == nil {
		panic("isClientShutDown is nil")
	}
}

type ConnectionManager struct {
	nextConnID           int64 // This field should be at the top: https://pkg.go.dev/sync/atomic#pkg-note-BUG
	logger               logger.LogAdaptor
	isClientShutDown     func() bool
	failoverConfig       *pubcluster.FailoverConfig
	partitionService     *PartitionService
	serializationService *iserialization.Service
	eventDispatcher      *event.DispatchService
	invocationFactory    *ConnectionInvocationFactory
	clusterService       *Service
	invocationService    *invocation.Service
	connMap              *connectionMap
	doneChMu             *sync.RWMutex
	doneCh               chan struct{}
	clusterIDMu          *sync.Mutex
	clusterID            *types.UUID
	prevClusterID        *types.UUID
	failoverService      *FailoverService
	randGen              *rand.Rand
	clientName           string
	labels               []string
	clientUUID           types.UUID
	state                int32
	smartRouting         bool
}

func NewConnectionManager(bundle ConnectionManagerCreationBundle) *ConnectionManager {
	bundle.Check()
	manager := &ConnectionManager{
		clusterService:       bundle.ClusterService,
		partitionService:     bundle.PartitionService,
		serializationService: bundle.SerializationService,
		eventDispatcher:      bundle.EventDispatcher,
		invocationFactory:    bundle.InvocationFactory,
		isClientShutDown:     bundle.IsClientShutdown,
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
		doneChMu:             &sync.RWMutex{},
	}
	return manager
}

func (m *ConnectionManager) Start(ctx context.Context) error {
	m.reset()
	return m.start(ctx)
}

// SetInvocationService sets the invocation service for the connection manager.
// This method should be called before Start.
func (m *ConnectionManager) SetInvocationService(s *invocation.Service) {
	m.invocationService = s
}

func (m *ConnectionManager) ClientUUID() types.UUID {
	return m.clientUUID
}

func (m *ConnectionManager) start(ctx context.Context) error {
	atomic.StoreInt32(&m.state, starting)
	m.logger.Trace(func() string { return "cluster.ConnectionManager.start" })
	var addr pubcluster.Address
	var err error
	if m.smartRouting {
		if addr, err = m.startSmart(ctx); err != nil {
			return err
		}
	} else if addr, err = m.startUnisocket(ctx); err != nil {
		return err
	}
	m.checkClusterIDChanged()
	m.eventDispatcher.Publish(NewConnected(addr))
	m.eventDispatcher.Publish(lifecycle.NewLifecycleStateChanged(lifecycle.StateConnected))
	atomic.StoreInt32(&m.state, ready)
	m.eventDispatcher.Subscribe(EventConnection, connectionManagerSubID, m.handleConnectionEvent)
	return nil
}

func (m *ConnectionManager) startUnisocket(ctx context.Context) (pubcluster.Address, error) {
	// need to wait for the initial member list even for the unisocket client
	// to prevent errors with stuff that requires the cluster service to have members, such as PNCounter
	ch := make(chan struct{})
	once := &sync.Once{}
	m.eventDispatcher.Subscribe(EventMembers, connectionManagerSubID, func(e event.Event) {
		m.handleMembersEvent(e)
		once.Do(func() {
			close(ch)
		})
	})
	addr, err := m.tryConnectCluster(ctx)
	if err != nil {
		return "", err
	}
	if err = m.waitInitialMemberList(ctx, ch); err != nil {
		return "", err
	}
	m.eventDispatcher.Unsubscribe(EventMembers, connectionManagerSubID)
	return addr, err
}

func (m *ConnectionManager) startSmart(ctx context.Context) (pubcluster.Address, error) {
	ch := make(chan struct{})
	once := &sync.Once{}
	m.eventDispatcher.Subscribe(EventMembers, connectionManagerSubID, func(e event.Event) {
		once.Do(func() {
			m.connectAllMembers(ctx)
			close(ch)
		})
	})
	addr, err := m.tryConnectCluster(ctx)
	if err != nil {
		return "", err
	}
	if err = m.waitInitialMemberList(ctx, ch); err != nil {
		return "", err
	}
	// fix broken connections only in the smart mode
	go m.syncConnections()
	return addr, nil
}

func (m *ConnectionManager) waitInitialMemberList(ctx context.Context, ch chan struct{}) error {
	m.logger.Debug(func() string { return "cluster.ConnectionManager.start: waiting for the initial member list" })
	timer := time.NewTimer(initialMembersTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("getting initial member list from cluster: %w", ctx.Err())
	case <-timer.C:
		return fmt.Errorf("timed out getting initial member list from cluster: %w", hzerrors.ErrIllegalState)
	case <-ch:
		break
	}
	m.logger.Debug(func() string { return "cluster.ConnectionManager.start: received the initial member list" })
	return nil
}

func (m *ConnectionManager) checkClusterIDChanged() {
	// check if cluster ID has changed after reconnection
	m.clusterIDMu.Lock()
	clusterIDChanged := m.prevClusterID != nil && m.prevClusterID != m.clusterID
	m.clusterIDMu.Unlock()
	if clusterIDChanged {
		m.eventDispatcher.Publish(lifecycle.NewLifecycleStateChanged(lifecycle.StateChangedCluster))
	}
}

// ClusterID returns the id of the connected cluster at that moment.
// If the client is not connected to any cluster yet, it returns an empty UUID.
func (m *ConnectionManager) ClusterID() types.UUID {
	m.clusterIDMu.Lock()
	defer m.clusterIDMu.Unlock()
	clusterID := m.clusterID
	if clusterID == nil {
		return types.UUID{}
	}
	return *clusterID
}

func (m *ConnectionManager) Stop() {
	m.eventDispatcher.Unsubscribe(EventConnection, connectionManagerSubID)
	m.eventDispatcher.Unsubscribe(EventMembers, connectionManagerSubID)
	if !atomic.CompareAndSwapInt32(&m.state, ready, stopped) {
		return
	}
	m.doneChMu.RLock()
	close(m.doneCh)
	m.doneChMu.RUnlock()
	m.connMap.CloseAll(nil)
}

func (m *ConnectionManager) NextConnectionID() int64 {
	return atomic.AddInt64(&m.nextConnID, 1)
}

func (m *ConnectionManager) GetConnectionForUUID(uuid types.UUID) *Connection {
	return m.connMap.GetConnectionForUUID(uuid)
}

func (m *ConnectionManager) GetConnectionForAddress(addr pubcluster.Address) *Connection {
	return m.connMap.GetConnectionForAddr(addr)
}

func (m *ConnectionManager) GetConnectionForPartition(partitionID int32) *Connection {
	if partitionID < 0 {
		panic("partition ID is negative")
	}
	uuid, ok := m.partitionService.GetPartitionOwner(partitionID)
	if !ok {
		return nil
	}
	return m.connMap.GetConnectionForUUID(uuid)
}

func (m *ConnectionManager) ActiveConnections() []*Connection {
	return m.connMap.ActiveConnections()
}

func (m *ConnectionManager) RandomConnection() *Connection {
	return m.connMap.RandomConn()
}

func (m *ConnectionManager) SQLConnection() *Connection {
	if m.smartRouting {
		if member := m.clusterService.SQLMember(); member != nil {
			if conn := m.GetConnectionForUUID(member.UUID); conn != nil {
				return conn
			}
		}
	}
	// Return the first member that's not to a lite member.
	if conn := m.someNonLiteConnection(); conn != nil {
		return conn
	}
	// All else failed, return a random connection, even if it is a lite member.
	return m.connMap.RandomConn()
}

func (m *ConnectionManager) someNonLiteConnection() *Connection {
	members := m.clusterService.OrderedMembers()
	for _, mem := range members {
		if mem.LiteMember {
			continue
		}
		if conn := m.connMap.GetConnectionForUUID(mem.UUID); conn != nil {
			return conn
		}
	}
	return nil
}

func (m *ConnectionManager) reset() {
	m.doneChMu.Lock()
	m.doneCh = make(chan struct{})
	m.doneChMu.Unlock()
	m.clusterIDMu.Lock()
	if m.clusterID != nil {
		m.prevClusterID = m.clusterID
	}
	m.clusterID = nil
	m.clusterIDMu.Unlock()
	m.connMap.Reset()
}

func (m *ConnectionManager) handleMembersEvent(event event.Event) {
	e := event.(*MembersStateChangedEvent)
	if e.State == MembersStateRemoved {
		m.handleMembersRemoved(e)
	}
}

func (m *ConnectionManager) handleMembersRemoved(e *MembersStateChangedEvent) {
	m.logger.Trace(func() string {
		return fmt.Sprintf("cluster.ConnectionManager.handleMembersRemoved: %v", e.Members)
	})
	for _, conn := range m.connMap.FindExtraConns(e.Members) {
		m.removeConnection(conn)
	}
}

func (m *ConnectionManager) handleConnectionEvent(event event.Event) {
	if atomic.LoadInt32(&m.state) != ready {
		return
	}
	e := event.(*ConnectionStateChangedEvent)
	if e.state == ConnectionStateOpened {
		return
	}
	m.removeConnection(e.Conn)
}

func (m *ConnectionManager) removeConnection(conn *Connection) int {
	remaining := m.connMap.RemoveConnection(conn)
	if remaining == 0 {
		m.eventDispatcher.Publish(NewDisconnected())
		m.eventDispatcher.Publish(lifecycle.NewLifecycleStateChanged(lifecycle.StateDisconnected))
	}
	return remaining
}

func (m *ConnectionManager) tryConnectCluster(ctx context.Context) (pubcluster.Address, error) {
	tryCount := 1
	if m.failoverConfig.Enabled {
		tryCount = m.failoverConfig.TryCount
	}
	var addr pubcluster.Address
	var err error
	for i := 1; i <= tryCount; i++ {
		cluster := m.failoverService.Current()
		m.logger.Infof("trying to connect to cluster: %s", cluster.ClusterName)
		addr, err = m.tryConnectCandidateCluster(ctx, cluster)
		if err == nil {
			m.logger.Infof("connected to cluster: %s", cluster.ClusterName)
			return addr, nil
		}
		if nonRetryableConnectionErr(err) {
			break
		}
		m.failoverService.Next()
	}
	return "", ihzerrors.NewIllegalStateError("cannot connect to any cluster", err)
}

func (m *ConnectionManager) tryConnectCandidateCluster(ctx context.Context, cluster *CandidateCluster) (pubcluster.Address, error) {
	cs := cluster.ConnectionStrategy
	cbr := cb.NewCircuitBreaker(
		cb.MaxRetries(math.MaxInt32),
		cb.Timeout(time.Duration(cs.Timeout)),
		cb.MaxFailureCount(3),
		cb.RetryPolicy(makeRetryPolicy(m.randGen, &cs.Retry)),
	)
	addr, err := cbr.TryContext(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if m.isClientShutDown() {
			return nil, cb.WrapNonRetryableError(fmt.Errorf("client is shut down"))
		}
		addr, err := m.connectCluster(ctx, cluster)
		if err != nil {
			m.logger.Debug(func() string {
				return fmt.Sprintf("cluster.ConnectionManager: error connecting to cluster, attempt %d: %s", attempt+1, err.Error())
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
		initialAddr, err = m.tryConnectAddress(ctx, addr, connectMember, cluster.NetworkCfg)
		if err != nil {
			continue
		}
	}
	if initialAddr == "" {
		return "", err
	}
	return initialAddr, nil
}

func (m *ConnectionManager) ensureConnection(ctx context.Context, addr pubcluster.Address, networkCfg *pubcluster.NetworkConfig) (*Connection, error) {
	conn := m.createDefaultConnection()
	if err := conn.start(networkCfg, addr); err != nil {
		return nil, ihzerrors.NewTargetDisconnectedError(err.Error(), err)
	}
	if err := m.authenticate(ctx, conn); err != nil {
		conn.close(nil)
		return nil, err
	}
	return conn, nil
}

func (m *ConnectionManager) createDefaultConnection() *Connection {
	return &Connection{
		invocationService: m.invocationService,
		pending:           make(chan invocation.Invocation, 1024),
		doneCh:            make(chan struct{}),
		connectionID:      m.NextConnectionID(),
		eventDispatcher:   m.eventDispatcher,
		status:            starting,
		logger:            m.logger,
	}
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
	inv := m.invocationFactory.NewConnectionBoundInvocation(request, conn, nil, time.Now())
	m.logger.Debug(func() string {
		return fmt.Sprintf("authentication correlation ID: %d", inv.Request().CorrelationID())
	})
	if err := m.invocationService.SendRequest(ctx, inv); err != nil {
		return fmt.Errorf("authenticating: %w", err)
	}
	result, err := inv.GetWithContext(ctx)
	if err != nil {
		return err
	}
	conn, err = m.processAuthenticationResult(conn, result)
	if err != nil {
		return err
	}
	m.eventDispatcher.Publish(NewConnectionOpened(conn))
	return nil
}

func (m *ConnectionManager) processAuthenticationResult(conn *Connection, result *proto.ClientMessage) (*Connection, error) {
	status, address, uuid, _, serverHazelcastVersion, partitionCount, newClusterID, failoverSupported := codec.DecodeClientAuthenticationResponse(result)
	if m.failoverConfig.Enabled && !failoverSupported {
		m.logger.Warnf("cluster does not support failover: this feature is available in Hazelcast Enterprise")
		status = notAllowedInCluster
	}
	switch status {
	case authenticated:
		conn.setConnectedServerVersion(serverHazelcastVersion)
		conn.setMemberUUID(uuid)
		if err := m.partitionService.checkAndSetPartitionCount(partitionCount); err != nil {
			return nil, err
		}
		m.logger.Debug(func() string {
			return fmt.Sprintf("cluster.ConnectionManager: checking the cluster: %v, current cluster: %v", newClusterID, m.clusterID)
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
			return nil, fmt.Errorf("connection does not belong to this cluster: %w", hzerrors.ErrIllegalState)
		}
		if m.connMap.IsEmpty() {
			// the first connection that opens a connection to the new cluster should set clusterID
			m.clusterID = &newClusterID
		}
		m.clusterIDMu.Unlock()
		if oldConn, ok := m.connMap.GetOrAddConnection(conn, *address); !ok {
			// there is already a connection to this member
			m.logger.Warnf("duplicate connection to the same member with UUID: %s", conn.MemberUUID())
			conn.close(nil)
			return oldConn, nil
		}
		m.logger.Debug(func() string {
			return fmt.Sprintf("opened connection to: %s", *address)
		})
		return conn, nil
	case credentialsFailed:
		return nil, fmt.Errorf("invalid credentials: %w", hzerrors.ErrAuthentication)
	case serializationVersionMismatch:
		return nil, fmt.Errorf("serialization version mismatches with the server: %w", hzerrors.ErrAuthentication)
	case notAllowedInCluster:
		return nil, cb.WrapNonRetryableError(hzerrors.ErrClientNotAllowedInCluster)
	}
	return nil, hzerrors.ErrAuthentication
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

func (m *ConnectionManager) syncConnections() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.getDoneCh():
			return
		case <-ticker.C:
			m.connectAllMembers(context.Background())
		}
	}
}

func (m *ConnectionManager) getDoneCh() <-chan struct{} {
	m.doneChMu.RLock()
	ch := m.doneCh
	m.doneChMu.RUnlock()
	return ch
}

func (m *ConnectionManager) networkConfig() *pubcluster.NetworkConfig {
	return m.failoverService.Current().NetworkCfg
}

func (m *ConnectionManager) connectAllMembers(ctx context.Context) {
	for _, mem := range m.clusterService.OrderedMembers() {
		if err := m.tryConnectMember(ctx, &mem); err != nil {
			m.logger.Errorf("connecting member %s: %w", mem, err)
		}
	}
}

func (m *ConnectionManager) tryConnectAddress(ctx context.Context, addr pubcluster.Address, mf connectMemberFunc, networkCfg *pubcluster.NetworkConfig) (pubcluster.Address, error) {
	host, port, err := internal.ParseAddr(addr.String())
	if err != nil {
		return "", fmt.Errorf("parsing address: %w", err)
	}
	var finalAddr pubcluster.Address
	var finalErr error
	if port == 0 {
		// try all addresses in port range
		for _, c := range EnumerateAddresses(host, networkCfg.PortRange) {
			if a, err := mf(ctx, m, c, networkCfg); err != nil {
				finalErr = err
			} else {
				finalAddr = a
				break
			}
		}
	} else {
		finalAddr, finalErr = mf(ctx, m, addr, networkCfg)
	}
	if finalAddr == "" {
		return finalAddr, fmt.Errorf("cannot connect to any address in the cluster: %w", finalErr)
	}
	return finalAddr, nil
}

func (m *ConnectionManager) tryConnectMember(ctx context.Context, member *pubcluster.MemberInfo) error {
	uuid := member.UUID
	if conn := m.connMap.GetConnectionForUUID(uuid); conn != nil {
		return nil
	}
	if !m.connMap.CheckAddCandidate(uuid) {
		return nil
	}
	defer m.connMap.RemoveCandidate(uuid)
	addr, err := m.clusterService.TranslateMember(ctx, member)
	if err != nil {
		return err
	}
	if _, err := connectMember(ctx, m, addr, m.networkConfig()); err != nil {
		return err
	}
	return nil
}

type connectionMap struct {
	lb pubcluster.LoadBalancer
	mu *sync.RWMutex
	// addrToConn maps connection address to connection
	addrToConn map[pubcluster.Address]*Connection
	addrs      []pubcluster.Address
	uuidToConn map[types.UUID]*Connection
	candidates map[types.UUID]struct{}
}

func newConnectionMap(lb pubcluster.LoadBalancer) *connectionMap {
	return &connectionMap{
		lb:         lb,
		mu:         &sync.RWMutex{},
		addrToConn: map[pubcluster.Address]*Connection{},
		uuidToConn: map[types.UUID]*Connection{},
		candidates: map[types.UUID]struct{}{},
	}
}

func (m *connectionMap) Reset() {
	m.mu.Lock()
	m.addrToConn = map[pubcluster.Address]*Connection{}
	m.uuidToConn = map[types.UUID]*Connection{}
	m.candidates = map[types.UUID]struct{}{}
	m.addrs = nil
	m.mu.Unlock()
}

// GetOrAddConnection adds the connection if it doesn't already exist or returns the existent connection.
// ok is true if the connection was added.
func (m *connectionMap) GetOrAddConnection(conn *Connection, addr pubcluster.Address) (retConn *Connection, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// if the connection was already added, skip it
	if old, ok := m.uuidToConn[conn.MemberUUID()]; ok {
		return old, false
	}
	m.uuidToConn[conn.MemberUUID()] = conn
	m.addrToConn[addr] = conn
	m.addrs = append(m.addrs, addr)
	return conn, true
}

// RemoveConnection removes a connection and returns the number of remaining connections.
func (m *connectionMap) RemoveConnection(removedConn *Connection) int {
	m.mu.Lock()
	var remaining int
	for addr, conn := range m.addrToConn {
		if conn.connectionID == removedConn.connectionID {
			delete(m.addrToConn, addr)
			delete(m.uuidToConn, conn.MemberUUID())
			m.removeAddr(addr)
			break
		}
	}
	remaining = len(m.uuidToConn)
	m.mu.Unlock()
	return remaining
}

func (m *connectionMap) CloseAll(err error) {
	m.mu.RLock()
	for _, conn := range m.uuidToConn {
		conn.close(err)
	}
	m.mu.RUnlock()
}

func (m *connectionMap) GetConnectionForUUID(uuid types.UUID) *Connection {
	m.mu.RLock()
	conn := m.uuidToConn[uuid]
	m.mu.RUnlock()
	return conn
}

func (m *connectionMap) GetConnectionForAddr(addr pubcluster.Address) *Connection {
	m.mu.RLock()
	conn := m.addrToConn[addr]
	m.mu.RUnlock()
	return conn
}

func (m *connectionMap) RandomConn() *Connection {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.addrs) == 0 {
		return nil
	}
	var addr pubcluster.Address
	if len(m.addrs) == 1 {
		addr = m.addrs[0]
	} else {
		// load balancer mutates its own state
		// so OneOf should be called under write lock
		addr = m.lb.OneOf(m.addrs)
	}
	conn := m.addrToConn[addr]
	if conn != nil && conn.isAlive() {
		return conn
	}
	// if the connection was not found by using the load balancer, select the first open one.
	for _, conn = range m.uuidToConn {
		// Go randomizes maps, this is random enough.
		if conn.isAlive() {
			return conn
		}
	}
	return nil
}

func (m *connectionMap) ActiveConnections() []*Connection {
	m.mu.RLock()
	conns := make([]*Connection, 0, len(m.uuidToConn))
	for _, conn := range m.uuidToConn {
		if conn.isAlive() {
			conns = append(conns, conn)
		}
	}
	m.mu.RUnlock()
	return conns
}

func (m *connectionMap) IsEmpty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.uuidToConn) == 0
}

func (m *connectionMap) FindAddedMembers(members []pubcluster.MemberInfo, cs *Service) []pubcluster.MemberInfo {
	m.mu.RLock()
	addedMembers := make([]pubcluster.MemberInfo, 0, len(members))
	for _, member := range members {
		if _, exists := m.uuidToConn[member.UUID]; exists {
			continue
		}
		addedMembers = append(addedMembers, member)
	}
	m.mu.RUnlock()
	return addedMembers
}

// FindExtraConns returns connections to non-existent members.
func (m *connectionMap) FindExtraConns(members []pubcluster.MemberInfo) []*Connection {
	m.mu.RLock()
	conns := []*Connection{}
	for _, member := range members {
		if conn, exists := m.uuidToConn[member.UUID]; exists {
			conns = append(conns, conn)
		}
	}
	m.mu.RUnlock()
	return conns
}

// CheckAddCandidate checks whether there is a connection attempt to given member with UUID in progress.
// If there is, returns false.
// Otherwise, returns true.
func (m *connectionMap) CheckAddCandidate(uuid types.UUID) bool {
	// do not allow connection for member with UUID if there is one in in progress
	m.mu.RLock()
	_, ok := m.candidates[uuid]
	m.mu.RUnlock()
	if ok {
		// the connection attempt is in progress
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok = m.candidates[uuid]
	if ok {
		// the connection attempt is in progress
		return false
	}
	m.candidates[uuid] = struct{}{}
	return true
}

func (m *connectionMap) RemoveCandidate(uuid types.UUID) {
	m.mu.Lock()
	delete(m.candidates, uuid)
	m.mu.Unlock()
}

func (m *connectionMap) Len() int {
	m.mu.RLock()
	l := len(m.uuidToConn)
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

func connectMember(ctx context.Context, m *ConnectionManager, addr pubcluster.Address, networkCfg *pubcluster.NetworkConfig) (pubcluster.Address, error) {
	var initialAddr pubcluster.Address
	var err error
	if _, err = m.ensureConnection(ctx, addr, networkCfg); err != nil {
		m.logger.Errorf("cannot connect to %s: %w", addr.String(), err)
	} else if initialAddr == "" {
		initialAddr = addr
	}
	if initialAddr == "" {
		return "", fmt.Errorf("cannot connect to address in the cluster: %w", err)
	}
	return initialAddr, nil
}

func EnumerateAddresses(host string, portRange pubcluster.PortRange) []pubcluster.Address {
	var addrs []pubcluster.Address
	for i := portRange.Min; i <= portRange.Max; i++ {
		addrs = append(addrs, pubcluster.NewAddress(host, int32(i)))
	}
	return addrs
}

// FilterConns removes connections which do not conform to the given criteria.
// Moodifies conns slice.
// The order of connections in conns may change.
func FilterConns(conns []*Connection, ok func(conn *Connection) bool) []*Connection {
	/*
			this function efficiently removes non-member connections, by moving non-members to the end of the slice and finally returning a smaller slice which does not contain the non-members part.
			Example:
			M: member
			X: non-member
			i = 0, r = 0
			       v (i)
			cs = [ M X X M M ]
			               ^ (len(cs)-1-r)
			i = 1, r = 0
		             v (i)
			cs = [ M X X M M ]
						   ^ (len(cs)-1-r)
			swap:
			cs = [ M M X M X ]
			i = 1, r = 1
					 v (i)
			cs = [ M M X M X ]
			             ^ (len(cs)-1-r)
			i = 2, r = 1
				       v (i)
			cs = [ M M X M X ]
			             ^ (len(cs)-1-r)
			swap:
			cs = [ M M M X X ]
	*/
	var i, r int
	for {
		if i >= len(conns)-r {
			break
		}
		conn := conns[i]
		if !ok(conn) {
			// move not ok conn towards the end of the slice by swapping it with another conn
			// swapped conn will be ok checked in the next iteration
			conns[i], conns[len(conns)-1-r] = conns[len(conns)-1-r], conns[i]
			r++
			continue
		}
		i++
	}
	// return the ok slice
	return conns[:len(conns)-r]
}
