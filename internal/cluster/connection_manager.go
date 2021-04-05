package cluster

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"
	ilifecycle "github.com/hazelcast/hazelcast-go-client/v4/internal/lifecycle"

	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/security"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization/spi"
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
	PartitionService     *PartitionServiceImpl
	SerializationService spi.SerializationService
	EventDispatcher      event.DispatchService
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
	partitionService     *PartitionServiceImpl
	serializationService spi.SerializationService
	eventDispatcher      event.DispatchService
	clusterConfig        *pubcluster.Config
	credentials          security.Credentials
	heartbeatTimeout     time.Duration
	clientName           string
	invocationTimeout    time.Duration

	connMap           *connectionMap
	nextConnectionID  int64
	addressTranslator pubcluster.AddressTranslator
	smartRouting      bool
	alive             atomic.Value
	logger            logger.Logger
	started           atomic.Value
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
		clusterConfig:        bundle.ClusterConfig,
		credentials:          bundle.Credentials,
		clientName:           bundle.ClientName,
		invocationTimeout:    120 * time.Second,
		heartbeatTimeout:     60 * time.Second,
		connMap:              newConnectionMap(),
		addressTranslator:    bundle.AddressTranslator,
		smartRouting:         bundle.SmartRouting,
		logger:               bundle.Logger,
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
	m.eventDispatcher.Publish(ilifecycle.NewStateChangedImpl(lifecycle.StateClientConnected))
	return nil
}

func (m *ConnectionManager) Stop() chan struct{} {
	doneCh := make(chan struct{}, 1)
	if m.started.Load() == false {
		doneCh <- struct{}{}
	} else {
		go func() {
			connectionClosedSubscriptionID := event.MakeSubscriptionID(m.handleConnectionClosed)
			m.eventDispatcher.Unsubscribe(EventConnectionClosed, connectionClosedSubscriptionID)
			memberAddedSubscriptionID := event.MakeSubscriptionID(m.handleMembersAdded)
			m.eventDispatcher.Unsubscribe(EventMembersAdded, memberAddedSubscriptionID)
			m.connMap.CloseAll()
			m.started.Store(false)
			doneCh <- struct{}{}
		}()
	}
	return doneCh
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
	m.connMap.RemoveConnection(conn)
}

func (m *ConnectionManager) connectCluster() error {
	for _, addr := range m.clusterService.memberCandidateAddrs() {
		if err := m.connectAddr(addr); err == nil {
			return nil
		} else {
			m.logger.Info(fmt.Sprintf("cannot connect to %s", addr.String()), err)
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
	inv := NewConnectionBoundInvocation(request, -1, nil, connection, m.invocationTimeout)
	m.requestCh <- inv
	if result, err := inv.GetWithTimeout(m.heartbeatTimeout); err != nil {
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
			m.logger.Info("Setting ", conn, " as owner.")
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
	// TODO: use credentials from config
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

type AuthenticationDecoder func(clientMessage *proto.ClientMessage) (
	status uint8,
	address pubcluster.Address,
	uuid internal.UUID,
	ownerUuid internal.UUID,
	serializationVersion uint8,
	serverHazelcastVersion string,
	partitionCount int32,
	clientUnregisteredMembers []pubcluster.Member)

func (cm *ConnectionManager) getAuthenticationDecoder() AuthenticationDecoder {
	var authenticationDecoder AuthenticationDecoder
	if _, ok := cm.credentials.(*security.UsernamePasswordCredentials); ok {
		authenticationDecoder = proto.DecodeClientAuthenticationResponse
	} else {
		// TODO: rename proto.ClientAuthenticationCustomDecodeResponse
		authenticationDecoder = proto.ClientAuthenticationCustomDecodeResponse
	}
	return authenticationDecoder
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

func (m *connectionMap) RemoveConnection(removedConn *Connection) {
	m.connectionsMu.Lock()
	defer m.connectionsMu.Unlock()
	for addr, conn := range m.connections {
		if conn.connectionID == removedConn.connectionID {
			delete(m.connections, addr)
			delete(m.connToAddr, conn.connectionID)
			break
		}
	}
}

func (m *connectionMap) CloseAll() {
	m.connectionsMu.Lock()
	defer m.connectionsMu.Unlock()
	for _, conn := range m.connections {
		conn.close(nil)
	}
	m.connections = map[string]*Connection{}
	m.connToAddr = map[int64]pubcluster.Address{}
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
	conns := make([]*Connection, 0, len(m.connections))
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
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
