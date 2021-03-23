package cluster

import (
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"
	ilifecycle "github.com/hazelcast/hazelcast-go-client/v4/internal/lifecycle"
	"sync"
	"sync/atomic"
	"time"

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
	NetworkConfig        pubcluster.NetworkConfig
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
	if b.NetworkConfig == nil {
		panic("NetworkConfig is nil")
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
	networkConfig        pubcluster.NetworkConfig
	credentials          security.Credentials
	heartbeatTimeout     time.Duration
	clientName           string
	invocationTimeout    time.Duration

	connectionsMu *sync.RWMutex
	// connections maps connection address to connection
	connections map[string]*Connection

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
		networkConfig:        bundle.NetworkConfig,
		credentials:          bundle.Credentials,
		clientName:           bundle.ClientName,
		invocationTimeout:    120 * time.Second,
		heartbeatTimeout:     60 * time.Second,
		connectionsMu:        &sync.RWMutex{},
		connections:          map[string]*Connection{},
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
	memberAddedSubscriptionID := event.MakeSubscriptionID(m.handleMemberAdded)
	m.eventDispatcher.Subscribe(EventMemberAdded, memberAddedSubscriptionID, m.handleMemberAdded)
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
			memberAddedSubscriptionID := event.MakeSubscriptionID(m.handleMemberAdded)
			m.eventDispatcher.Unsubscribe(EventMemberAdded, memberAddedSubscriptionID)
			m.connectionsMu.Lock()
			for _, conn := range m.connections {
				conn.close(nil)
			}
			m.connections = map[string]*Connection{}
			m.connectionsMu.Unlock()
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
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	if conn, ok := m.connections[addr.String()]; ok {
		return conn
	}
	return nil
}

func (m *ConnectionManager) GetActiveConnections() []*Connection {
	conns := make([]*Connection, 0, len(m.connections))
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	for _, conn := range m.connections {
		conns = append(conns, conn)
	}
	return conns
}

func (m *ConnectionManager) handleMemberAdded(event event.Event) {
	if m.started.Load() != true {
		return
	}
	if memberAddedEvent, ok := event.(MemberAdded); ok {
		member := memberAddedEvent.Member()
		addr := member.Address()
		m.logger.Info("connectionManager member added", addr.String())
		m.connectionsMu.RLock()
		// TODO: check whether m.connections is not nil
		_, exists := m.connections[addr.String()]
		m.connectionsMu.RUnlock()
		if !exists {
			if err := m.connectAddr(addr); err != nil {
				m.logger.Errorf("error connecting addr: %w", err)
			}
		}
	}
}

func (m *ConnectionManager) handleConnectionClosed(event event.Event) {
	if m.started.Load() != true {
		return
	}
	if connectionClosedEvent, ok := event.(ConnectionClosed); ok {
		if err := connectionClosedEvent.Err(); err != nil {
			panic("implement me!")
		}
		m.connectionsMu.Lock()
		defer m.connectionsMu.Unlock()
		eventConn := connectionClosedEvent.Conn()
		for addr, conn := range m.connections {
			if conn.connectionID == eventConn.connectionID {
				delete(m.connections, addr)
				break
			}
		}
	}
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
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	if conn, found := m.connections[addr.String()]; found {
		return conn
	}
	return nil
}

func (m *ConnectionManager) maybeCreateConnection(addr pubcluster.Address, owner bool) (*Connection, error) {
	//// check whether the connection exists before creating it
	//if conn, found := m.connections[addr.String()]; found {
	//	return conn, nil
	//}
	// TODO: check whether we can create a connection
	conn := m.createDefaultConnection()
	if err := conn.start(m.networkConfig, addr); err != nil {
		return nil, hzerror.NewHazelcastTargetDisconnectedError(err.Error(), err)
	} else if err = m.authenticate(conn, owner); err != nil {
		return nil, err
	}
	return conn, nil
	//} else {
	//	m.eventDispatcher.Publish(NewConnectionOpened(conn))
	//	m.connectionsMu.Lock()
	//	defer m.connectionsMu.Unlock()
	//	m.connections[addr.String()] = conn
	//	return conn, nil
}

func (m *ConnectionManager) createDefaultConnection() *Connection {
	//builder := &clientMessageBuilder{
	//	handleResponse:     m.invocationService.HandleResponse,
	//	incompleteMessages: make(map[int64]*proto.ClientMessage),
	//}
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
	status, address, memberUuid, _, serverHazelcastVersion, partitionCount, _, _ := codec.DecodeClientAuthenticationResponse(result)
	switch status {
	case authenticated:
		conn.setConnectedServerVersion(serverHazelcastVersion)
		conn.endpoint.Store(address)
		conn.isOwnerConnection = asOwner
		m.connections[address.String()] = conn
		m.eventDispatcher.Publish(NewConnectionOpened(conn))
		if asOwner {
			m.partitionService.checkAndSetPartitionCount(partitionCount)
			m.clusterService.ownerConnectionAddr.Store(address)
			m.clusterService.ownerUUID.Store(memberUuid.String())
			m.clusterService.uuid.Store(memberUuid.String())
			m.logger.Info("Setting ", conn, " as owner.")
		}
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
	//uuid := m.client.ClusterService.uuid.Load().(string)
	//ownerUUID := m.client.ClusterService.ownerUUID.Load().(string)
	// TODO: use credentials from config
	return codec.EncodeClientAuthenticationRequest(
		"dev",
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

//type AuthenticationDecoder func(clientMessage *proto.ClientMessage) func() (status uint8, address *proto.Address,
//	uuid string, ownerUuid string, serializationVersion uint8, serverHazelcastVersion string,
//	clientUnregisteredMembers []*proto.Member)
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
