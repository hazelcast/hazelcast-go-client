package cluster

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	ilifecycle "github.com/hazelcast/hazelcast-go-client/internal/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/lifecycle"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/security"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization/spi"
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
	nextConnectionID  int64
	addressTranslator pubcluster.AddressTranslator
	smartRouting      bool
	alive             atomic.Value
	logger            logger.Logger
	started           atomic.Value
	doneCh            chan struct{}
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
		credentials:          bundle.Credentials,
		clientName:           bundle.ClientName,
		connMap:              newConnectionMap(),
		addressTranslator:    bundle.AddressTranslator,
		smartRouting:         bundle.SmartRouting,
		logger:               bundle.Logger,
		doneCh:               make(chan struct{}, 1),
	}
	manager.alive.Store(false)
	manager.started.Store(false)
	return manager
}

func (m *ConnectionManager) Start() error {
	if m.started.Load() == true {
		return nil
	}
	if err := m.connectCluster(); err != nil {
		return err
	}
	connectionClosedSubscriptionID := event.MakeSubscriptionID(m.handleConnectionClosed)
	m.eventDispatcher.Subscribe(EventConnectionClosed, connectionClosedSubscriptionID, m.handleConnectionClosed)
	memberAddedSubscriptionID := event.MakeSubscriptionID(m.handleMembersAdded)
	m.eventDispatcher.Subscribe(EventMembersAdded, memberAddedSubscriptionID, m.handleMembersAdded)
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
	return atomic.AddInt64(&m.nextConnectionID, 1)
}

func (m *ConnectionManager) GetConnectionForAddress(addr pubcluster.Address) *Connection {
	return m.connMap.GetConnForAddr(addr.String())
}

func (m *ConnectionManager) GetConnectionForPartition(partitionID int32) *Connection {
	if partitionID < 0 {
		panic("partition ID is negative")
	}
	if ownerUUID := m.partitionService.GetPartitionOwner(partitionID); ownerUUID == nil {
		return nil
	} else if member := m.clusterService.GetMemberByUUID(ownerUUID.String()); member == nil {
		return nil
	} else {
		return m.GetConnectionForAddress(member.Address())
	}
}

func (m *ConnectionManager) GetActiveConnections() []*Connection {
	return m.connMap.Connections()
}

func (m *ConnectionManager) handleMembersAdded(event event.Event) {
	if m.started.Load() != true {
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
			if addr := m.connMap.GetAddrForConnID(conn.connectionID); addr != nil {
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
	}
}

func (m *ConnectionManager) connectCluster() error {
	for _, addr := range m.clusterService.memberCandidateAddrs() {
		if err := m.connectAddr(addr); err == nil {
			return nil
		} else {
			m.logger.Infof("cannot connect to %s: %w", addr.String(), err)
		}
	}
	return errors.New("cannot connect to any address in the cluster")
}

func (m *ConnectionManager) connectAddr(addr pubcluster.Address) error {
	_, err := m.ensureConnection(addr, true)
	return err
}

func (m *ConnectionManager) ensureConnection(addr pubcluster.Address, owner bool) (*Connection, error) {
	if conn := m.getConnection(addr, owner); conn != nil {
		return conn, nil
	}
	addr = m.addressTranslator.Translate(addr)
	return m.maybeCreateConnection(addr, owner)
}

func (m *ConnectionManager) getConnection(addr pubcluster.Address, owner bool) *Connection {
	return m.GetConnectionForAddress(addr)
}

func (m *ConnectionManager) maybeCreateConnection(addr pubcluster.Address, owner bool) (*Connection, error) {
	// TODO: check whether we can create a connection
	conn := m.createDefaultConnection()
	if err := conn.start(m.clusterConfig, addr); err != nil {
		return nil, hzerror.NewHazelcastTargetDisconnectedError(err.Error(), err)
	} else if err = m.authenticate(conn, owner); err != nil {
		return nil, err
	}
	return conn, nil
}

func (m *ConnectionManager) createDefaultConnection() *Connection {
	return &Connection{
		responseCh:      m.responseCh,
		pending:         make(chan *proto.ClientMessage, 1),
		closed:          make(chan struct{}),
		readBuffer:      make([]byte, 0),
		connectionID:    m.NextConnectionID(),
		eventDispatcher: m.eventDispatcher,
		status:          0,
		logger:          m.logger,
	}
}

func (m *ConnectionManager) authenticate(connection *Connection, asOwner bool) error {
	m.credentials.SetEndpoint(connection.socket.LocalAddr().String())
	request := m.encodeAuthenticationRequest(asOwner)
	inv := m.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, connection, m.clusterConfig.InvocationTimeout)
	m.requestCh <- inv
	if result, err := inv.GetWithTimeout(m.clusterConfig.HeartbeatTimeout); err != nil {
		return err
	} else {
		return m.processAuthenticationResult(connection, asOwner, result)
	}
}

func (m *ConnectionManager) processAuthenticationResult(conn *Connection, asOwner bool, result *proto.ClientMessage) error {
	status, address, memberUuid, _, serverHazelcastVersion, partitionCount, clusterUUID, _ := codec.DecodeClientAuthenticationResponse(result)
	switch status {
	case authenticated:
		conn.setConnectedServerVersion(serverHazelcastVersion)
		conn.endpoint.Store(address)
		conn.isOwnerConnection = asOwner
		m.connMap.AddConnection(conn, address)
		if asOwner {
			// TODO: detect cluster change
			m.partitionService.checkAndSetPartitionCount(partitionCount)
			m.clusterService.ownerConnectionAddr.Store(address)
			m.clusterService.ownerUUID.Store(memberUuid.String())
			m.clusterService.uuid.Store(clusterUUID.String())
			m.logger.Infof("Setting %s as owner.", conn.String())
		}
		m.eventDispatcher.Publish(NewConnectionOpened(conn))
	case credentialsFailed:
		return hzerror.NewHazelcastAuthenticationError("invalid credentials!", nil)
	case serializationVersionMismatch:
		return hzerror.NewHazelcastAuthenticationError("serialization version mismatches with the server!", nil)
	}
	return nil
}

func (m *ConnectionManager) encodeAuthenticationRequest(asOwner bool) *proto.ClientMessage {
	if creds, ok := m.credentials.(*security.UsernamePasswordCredentials); ok {
		return m.createAuthenticationRequest(asOwner, creds)
	}
	return m.createCustomAuthenticationRequest(asOwner)

}

func (m *ConnectionManager) createAuthenticationRequest(asOwner bool,
	creds *security.UsernamePasswordCredentials) *proto.ClientMessage {
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

func (m *ConnectionManager) createCustomAuthenticationRequest(asOwner bool) *proto.ClientMessage {
	uuid := m.clusterService.uuid.Load().(string)
	ownerUUID := m.clusterService.ownerUUID.Load().(string)
	credsData, err := m.serializationService.ToData(m.credentials)
	if err != nil {
		m.logger.Error("Credentials cannot be serialized!")
		return nil
	}
	return proto.ClientAuthenticationCustomEncodeRequest(
		credsData,
		uuid,
		ownerUUID,
		asOwner,
		proto.ClientType,
		serializationVersion,
		ClientVersion,
	)
}

func (m *ConnectionManager) getAuthenticationDecoder() AuthenticationDecoder {
	var authenticationDecoder AuthenticationDecoder
	if _, ok := m.credentials.(*security.UsernamePasswordCredentials); ok {
		authenticationDecoder = proto.DecodeClientAuthenticationResponse
	} else {
		// TODO: rename proto.ClientAuthenticationCustomDecodeResponse
		authenticationDecoder = proto.ClientAuthenticationCustomDecodeResponse
	}
	return authenticationDecoder
}

func (m *ConnectionManager) heartbeat() {
	ticker := time.NewTicker(m.clusterConfig.HeartbeatInterval)
	for {
		select {
		case <-m.doneCh:
			return
		case <-ticker.C:
			for _, conn := range m.GetActiveConnections() {
				m.sendHeartbeat(conn)
			}
		}
	}
}

func (m *ConnectionManager) sendHeartbeat(conn *Connection) {
	request := codec.EncodeClientPingRequest()
	inv := m.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, conn, m.clusterConfig.HeartbeatTimeout)
	m.requestCh <- inv
}

func checkOwnerConn(owner bool, conn *Connection) bool {
	return owner && conn.isOwnerConnection
}

type connectionMap struct {
	connectionsMu *sync.RWMutex
	// connections maps connection address to connection
	connections map[string]*Connection
	// connToAddr maps connection ID to address
	connToAddr map[int64]pubcluster.Address
}

func newConnectionMap() *connectionMap {
	return &connectionMap{
		connectionsMu: &sync.RWMutex{},
		connections:   map[string]*Connection{},
		connToAddr:    map[int64]pubcluster.Address{},
	}
}

func (m *connectionMap) AddConnection(conn *Connection, addr pubcluster.Address) {
	m.connectionsMu.Lock()
	defer m.connectionsMu.Unlock()
	m.connections[addr.String()] = conn
	m.connToAddr[conn.connectionID] = addr
}

// RemoveConnectionDisconected removes a connection and returns true if there are no more connections left.
func (m *connectionMap) RemoveConnection(removedConn *Connection) int {
	var remaining int
	m.connectionsMu.Lock()
	for addr, conn := range m.connections {
		if conn.connectionID == removedConn.connectionID {
			delete(m.connections, addr)
			delete(m.connToAddr, conn.connectionID)
			remaining = len(m.connections)
			break
		}
	}
	m.connectionsMu.Unlock()
	return remaining
}

func (m *connectionMap) CloseAll() {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	for _, conn := range m.connections {
		conn.close(nil)
	}
}

func (m *connectionMap) GetConnForAddr(addr string) *Connection {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	if conn, ok := m.connections[addr]; ok {
		return conn
	}
	return nil
}

func (m *connectionMap) GetAddrForConnID(connID int64) pubcluster.Address {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	if addr, ok := m.connToAddr[connID]; ok {
		return addr
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

func (m *connectionMap) FindAddedAddrs(members []pubcluster.Member) []pubcluster.Address {
	addedAddrs := []pubcluster.Address{}
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	for _, member := range members {
		addr := member.Address()
		if _, exists := m.connections[addr.String()]; !exists {
			addedAddrs = append(addedAddrs, addr)
		}
	}
	return addedAddrs
}

func (m *connectionMap) FindRemovedConns(members []pubcluster.Member) []*Connection {
	removedConns := []*Connection{}
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	for _, member := range members {
		addr := member.Address()
		if conn, exists := m.connections[addr.String()]; exists {
			removedConns = append(removedConns, conn)
		}
	}
	return removedConns
}

type AuthenticationDecoder func(clientMessage *proto.ClientMessage) (
	status uint8,
	address pubcluster.Address,
	uuid internal.UUID,
	ownerUuid internal.UUID,
	serializationVersion uint8,
	serverHazelcastVersion string,
	partitionCount int32,
	clientUnregisteredMembers []pubcluster.Member)
