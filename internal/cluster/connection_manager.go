package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilifecycle "github.com/hazelcast/hazelcast-go-client/internal/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/security"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization/spi"
	"github.com/hazelcast/hazelcast-go-client/internal/util/nilutil"
	"github.com/hazelcast/hazelcast-go-client/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

const (
	authenticated = iota
	credentialsFailed
	serializationVersionMismatch
)

const serializationVersion = 1

// TODO: This should be replace with a build time version variable, BuildInfo etc.
var ClientVersion = "4.0.0"

type ConnectionManagerCreationBundle struct {
	RequestCh            chan<- invocation.Invocation
	ResponseCh           chan<- *proto.ClientMessage
	SmartRouting         bool
	Logger               logger.Logger
	AddressTranslator    pubcluster.AddressTranslator
	ClusterService       *ServiceImpl
	PartitionService     *PartitionService
	SerializationService spi.SerializationService
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
	if b.AddressTranslator == nil {
		panic("AddressTranslator is nil")
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
		panic("clientName is blank")
	}
}

type ConnectionManager struct {
	requestCh            chan<- invocation.Invocation
	responseCh           chan<- *proto.ClientMessage
	clusterService       *ServiceImpl
	partitionService     *PartitionService
	serializationService spi.SerializationService
	eventDispatcher      *event.DispatchService
	invocationFactory    *ConnectionInvocationFactory
	clusterConfig        *pubcluster.Config
	credentials          security.Credentials
	clientName           string

	connMap           *connectionMap
	nextConnID        int64
	addressTranslator pubcluster.AddressTranslator
	smartRouting      bool
	alive             atomic.Value
	started           atomic.Value
	ownerConn         atomic.Value
	logger            logger.Logger
	doneCh            chan struct{}
	cb                *cb.CircuitBreaker
}

func NewConnectionManager(bundle ConnectionManagerCreationBundle) *ConnectionManager {
	bundle.Check()
	// TODO: make circuit breaker configurable
	circuitBreaker := cb.NewCircuitBreaker(
		cb.MaxRetries(10),
		cb.MaxFailureCount(3),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration(attempt+1) * time.Second
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
		connMap:              newConnectionMap(),
		addressTranslator:    bundle.AddressTranslator,
		smartRouting:         bundle.SmartRouting,
		logger:               bundle.Logger,
		doneCh:               make(chan struct{}, 1),
		cb:                   circuitBreaker,
	}
	manager.alive.Store(false)
	manager.started.Store(false)
	return manager
}

func (m *ConnectionManager) Start() error {
	if m.started.Load() == true {
		return nil
	}
	if _, err := m.cb.Try(func(ctx context.Context) (interface{}, error) {
		return nil, m.connectCluster()
	}).Result(); err != nil {
		return err
	}
	connectionClosedSubscriptionID := event.MakeSubscriptionID(m.handleConnectionClosed)
	m.eventDispatcher.Subscribe(EventConnectionClosed, connectionClosedSubscriptionID, m.handleConnectionClosed)
	memberAddedSubscriptionID := event.MakeSubscriptionID(m.handleMembersAdded)
	m.eventDispatcher.Subscribe(EventMembersAdded, memberAddedSubscriptionID, m.handleMembersAdded)
	go m.logStatus()
	go m.doctor()
	m.started.Store(true)
	m.eventDispatcher.Publish(ilifecycle.NewStateChanged(lifecycle.StateClientConnected))
	return nil
}

func (m *ConnectionManager) Stop() chan struct{} {
	if m.started.Load() == true {
		go func() {
			connectionClosedSubscriptionID := event.MakeSubscriptionID(m.handleConnectionClosed)
			m.eventDispatcher.Unsubscribe(EventConnectionClosed, connectionClosedSubscriptionID)
			memberAddedSubscriptionID := event.MakeSubscriptionID(m.handleMembersAdded)
			m.eventDispatcher.Unsubscribe(EventMembersAdded, memberAddedSubscriptionID)
			m.connMap.CloseAll()
			m.started.Store(false)
			close(m.doneCh)
		}()
	}
	return m.doneCh
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

func (m *ConnectionManager) OwnerConnectionAddr() *pubcluster.AddressImpl {
	if conn, ok := m.ownerConn.Load().(*Connection); ok && conn != nil {
		return m.connMap.GetAddrForConnectionID(conn.connectionID)
	}
	return nil
}

func (m *ConnectionManager) handleMembersAdded(event event.Event) {
	if m.started.Load() != true {
		return
	}
	if memberAddedEvent, ok := event.(*MembersAdded); ok {
		missingAddrs := m.connMap.FindAddedAddrs(memberAddedEvent.Members)
		for _, addr := range missingAddrs {
			if err := m.connectAddr(addr); err != nil {
				m.logger.Errorf("error connecting addr: %s", err.Error())
			} else {
				m.logger.Infof("connectionManager member added: %s", addr.String())
			}
		}
	}
}

func (m *ConnectionManager) handleMembersRemoved(event event.Event) {
	if m.started.Load() != true {
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
	if m.started.Load() != true {
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
		m.eventDispatcher.Publish(ilifecycle.NewStateChanged(lifecycle.StateClientDisconnected))
		// set nil *Connection as owner conn
		var conn *Connection
		m.ownerConn.Store(conn)
	} else {
		ownerConn := m.ownerConn.Load().(*Connection)
		if ownerConn == nil || ownerConn.connectionID == conn.connectionID {
			m.assignNewOwner()
		}
	}
}

func (m *ConnectionManager) assignNewOwner() {
	if newOwner := m.RandomConnection(); newOwner != nil {
		m.logger.Infof("assigning new owner connection %s", newOwner.connectionID)
		m.ownerConn.Store(newOwner)
		m.eventDispatcher.Publish(NewOwnerConnectionChanged(newOwner))
	} else {
		m.logger.Warnf("could not assign new owner connection, no connections found")
	}
}

func (m *ConnectionManager) connectCluster() error {
	for _, addr := range m.clusterService.memberCandidateAddrs() {
		if err := m.connectAddr(addr); err == nil {
			return nil
		} else {
			m.logger.Errorf("cannot connect to %s: %w", addr.String(), err)
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
	addr = m.addressTranslator.Translate(addr).(*pubcluster.AddressImpl)
	return m.maybeCreateConnection(addr)
}

func (m *ConnectionManager) getConnection(addr *pubcluster.AddressImpl) *Connection {
	return m.GetConnectionForAddress(addr)
}

func (m *ConnectionManager) maybeCreateConnection(addr pubcluster.Address) (*Connection, error) {
	// TODO: check whether we can create a connection
	conn := m.createDefaultConnection()
	if err := conn.start(m.clusterConfig, addr); err != nil {
		return nil, hzerror.NewHazelcastTargetDisconnectedError(err.Error(), err)
	} else if err = m.authenticate(conn); err != nil {
		return nil, err
	}
	return conn, nil
}

func (m *ConnectionManager) createDefaultConnection() *Connection {
	return &Connection{
		responseCh:      m.responseCh,
		pending:         make(chan *proto.ClientMessage, 1),
		doneCh:          make(chan struct{}),
		readBuffer:      make([]byte, 0),
		connectionID:    m.NextConnectionID(),
		eventDispatcher: m.eventDispatcher,
		status:          0,
		logger:          m.logger,
	}
}

func (m *ConnectionManager) authenticate(connection *Connection) error {
	m.credentials.SetEndpoint(connection.socket.LocalAddr().String())
	request := m.encodeAuthenticationRequest()
	inv := m.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, connection, m.clusterConfig.InvocationTimeout, nil)
	m.requestCh <- inv
	if result, err := inv.GetWithTimeout(m.clusterConfig.HeartbeatTimeout); err != nil {
		return err
	} else {
		return m.processAuthenticationResult(connection, result)
	}
}

func (m *ConnectionManager) processAuthenticationResult(conn *Connection, result *proto.ClientMessage) error {
	status, address, memberUuid, _, serverHazelcastVersion, partitionCount, _, _ := codec.DecodeClientAuthenticationResponse(result)
	switch status {
	case authenticated:
		conn.setConnectedServerVersion(serverHazelcastVersion)
		conn.endpoint.Store(address)
		m.connMap.AddConnection(conn, address)
		if ownerConn, ok := m.ownerConn.Load().(*Connection); !ok || ownerConn == nil {
			// TODO: detect cluster change
			m.partitionService.checkAndSetPartitionCount(partitionCount)
			m.ownerConn.Store(conn)
			// TODO: remove v ?
			m.clusterService.ownerUUID.Store(memberUuid.String())
			m.logger.Infof("Setting %s as owner.", conn.String())
			m.eventDispatcher.Publish(NewOwnerConnectionChanged(conn))
		}
		m.logger.Infof("opened connection to: %s", address)
		m.eventDispatcher.Publish(NewConnectionOpened(conn))
	case credentialsFailed:
		return hzerror.NewHazelcastAuthenticationError("invalid credentials!", nil)
	case serializationVersionMismatch:
		return hzerror.NewHazelcastAuthenticationError("serialization version mismatches with the server!", nil)
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
		"",
		"",
		internal.NewUUID(),
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
	inv := m.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, conn, m.clusterConfig.HeartbeatTimeout, nil)
	m.requestCh <- inv
}

func (m *ConnectionManager) logStatus() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-m.doneCh:
			break
		case <-ticker.C:
			m.connMap.Info(func(connections map[string]*Connection, connToAddr map[int64]*pubcluster.AddressImpl) {
				m.logger.Trace(func() string {
					conns := map[string]int{}
					for addr, conn := range connections {
						conns[addr] = int(conn.connectionID)
					}
					return fmt.Sprintf("connections: %#v", conns)
				})
				m.logger.Trace(func() string {
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

func (m *ConnectionManager) doctor() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-m.doneCh:
			break
		case <-ticker.C:
			// TODO: very inefficent, fix this
			// find and connect the first connection which exists in the cluster but not in th connection manager
			for _, addrStr := range m.clusterService.MemberAddrs() {
				if conn := m.connMap.GetConnectionForAddr(addrStr); conn == nil {
					m.logger.Infof("found a broken connection to: %s, trying to fix it.", addrStr)
					if addr, err := ParseAddress(addrStr); err != nil {
						m.logger.Warnf("cannot parse address: %s", addrStr)
					} else if err := m.connectAddr(addr); err != nil {
						m.logger.Trace(func() string {
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
			// if the owner connection is not found, try to assign a new one.
			ownerConn := m.ownerConn.Load().(*Connection)
			if ownerConn == nil {
				m.assignNewOwner()
			}
		}
	}
}

type connectionMap struct {
	connectionsMu *sync.RWMutex
	// connections maps connection address to connection
	connections map[string]*Connection
	// connToAddr maps connection ID to address
	connToAddr map[int64]*pubcluster.AddressImpl
}

func newConnectionMap() *connectionMap {
	return &connectionMap{
		connectionsMu: &sync.RWMutex{},
		connections:   map[string]*Connection{},
		connToAddr:    map[int64]*pubcluster.AddressImpl{},
	}
}

func (m *connectionMap) AddConnection(conn *Connection, addr pubcluster.Address) {
	m.connectionsMu.Lock()
	defer m.connectionsMu.Unlock()
	m.connections[addr.String()] = conn
	m.connToAddr[conn.connectionID] = addr.(*pubcluster.AddressImpl)
}

// RemoveConnection removes a connection and returns true if there are no more connections left.
func (m *connectionMap) RemoveConnection(removedConn *Connection) int {
	m.connectionsMu.Lock()
	defer m.connectionsMu.Unlock()
	var remaining int
	for addr, conn := range m.connections {
		if conn.connectionID == removedConn.connectionID {
			delete(m.connections, addr)
			delete(m.connToAddr, conn.connectionID)
			remaining = len(m.connections)
			break
		}
	}
	return remaining
}

func (m *connectionMap) CloseAll() {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	for _, conn := range m.connections {
		conn.close(nil)
	}
}

func (m *connectionMap) GetConnectionForAddr(addr string) *Connection {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	if conn, ok := m.connections[addr]; ok {
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
	for _, conn := range m.connections {
		// get the first connection
		return conn
	}
	return nil
}

func (m *connectionMap) Connections() []*Connection {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	conns := make([]*Connection, 0, len(m.connections))
	for _, conn := range m.connections {
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
		if _, exists := m.connections[addr.String()]; !exists {
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
		if conn, exists := m.connections[addr.String()]; exists {
			removedConns = append(removedConns, conn)
		}
	}
	return removedConns
}

func (m *connectionMap) Info(infoFun func(connections map[string]*Connection, connToAddr map[int64]*pubcluster.AddressImpl)) {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	infoFun(m.connections, m.connToAddr)
}

func (m *connectionMap) Len() int {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	return len(m.connToAddr)
}
