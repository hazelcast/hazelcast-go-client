package cluster

import (
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/security"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization/spi"
	"sync"
	"sync/atomic"
	"time"
)

const (
	authenticated = iota
	credentialsFailed
	serializationVersionMismatch
)

const serializationVersion = 1

// TODO: This should be replace with a build time version variable, BuildInfo etc.
var ClientVersion = "4.0.0"

type ConnectionManager interface {
	AddListener(listener Listener)
	NextConnectionID() int64
	GetConnectionForAddress(addr *core.Address) *ConnectionImpl
	Start() error
	notifyConnectionClosed(connection *ConnectionImpl, cause error)
}

type ConnectionManagerCreationBundle struct {
	InvocationService    invocation.Service
	SmartRouting         bool
	Logger               logger.Logger
	AddressTranslator    internal.AddressTranslator
	ClusterService       *ServiceImpl
	PartitionService     *PartitionServiceImpl
	SerializationService spi.SerializationService
	NetworkConfig        NetworkConfig
	Credentials          security.Credentials
	ClientName           string
}

func (b ConnectionManagerCreationBundle) Check() {
	if b.InvocationService == nil {
		panic("InvocationService is nil")
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

type ConnectionManagerImpl struct {
	invocationService invocation.Service
	// TODO: depend on the interface
	clusterService       *ServiceImpl
	partitionService     *PartitionServiceImpl
	serializationService spi.SerializationService
	networkConfig        NetworkConfig
	credentials          security.Credentials
	heartbeatTimeout     time.Duration
	clientName           string

	connectionsMu *sync.RWMutex
	connections   map[string]*ConnectionImpl
	listenersMu   *sync.RWMutex
	listeners     []Listener

	nextConnectionID  int64
	addressTranslator internal.AddressTranslator
	smartRouting      bool
	alive             atomic.Value
	logger            logger.Logger
	started           bool
}

func NewConnectionManagerImpl(bundle ConnectionManagerCreationBundle) *ConnectionManagerImpl {
	bundle.Check()
	manager := &ConnectionManagerImpl{
		invocationService:    bundle.InvocationService,
		clusterService:       bundle.ClusterService,
		partitionService:     bundle.PartitionService,
		serializationService: bundle.SerializationService,
		networkConfig:        bundle.NetworkConfig,
		credentials:          bundle.Credentials,
		clientName:           bundle.ClientName,
		heartbeatTimeout:     60 * time.Second,
		connectionsMu:        &sync.RWMutex{},
		connections:          map[string]*ConnectionImpl{},
		listenersMu:          &sync.RWMutex{},
		addressTranslator:    bundle.AddressTranslator,
		smartRouting:         bundle.SmartRouting,
		logger:               bundle.Logger,
	}
	manager.alive.Store(true)
	return manager
}

func (m *ConnectionManagerImpl) Start() error {
	if m.started {
		return nil
	}
	if err := m.connectCluster(); err != nil {
		return err
	}
	m.started = true
	return nil
}

func (m *ConnectionManagerImpl) AddListener(listener Listener) {
	m.listenersMu.Lock()
	defer m.listenersMu.Unlock()
	m.listeners = append(m.listeners, listener)
}

func (m *ConnectionManagerImpl) NextConnectionID() int64 {
	return atomic.AddInt64(&m.nextConnectionID, 1)
}

func (m *ConnectionManagerImpl) GetConnectionForAddress(addr *core.Address) *ConnectionImpl {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	if conn, ok := m.connections[addr.String()]; ok {
		return conn
	}
	return nil
}

func (m *ConnectionManagerImpl) connectCluster() error {
	for _, addr := range m.clusterService.memberCandidateAddrs() {
		if err := m.connectAddr(addr); err == nil {
			return nil
		} else {
			m.logger.Info(fmt.Sprintf("cannot connect to %s", addr.String()), err)
		}
	}
	return errors.New("cannot connect to any adddress in the cluster")
}

func (m *ConnectionManagerImpl) connectAddr(addr *core.Address) error {
	_, err := m.ensureConnection(addr, true)
	return err
}

func (m *ConnectionManagerImpl) ensureConnection(addr *core.Address, owner bool) (*ConnectionImpl, error) {
	if conn := m.getConnection(addr, owner); conn != nil {
		return conn, nil
	}
	addr = m.addressTranslator.Translate(addr)
	return m.maybeCreateConnection(addr, owner)
}

func (m *ConnectionManagerImpl) getConnection(addr *core.Address, owner bool) *ConnectionImpl {
	m.connectionsMu.RLock()
	defer m.connectionsMu.RUnlock()
	if conn, found := m.connections[addr.String()]; found {
		return conn
	}
	return nil
}

func (m *ConnectionManagerImpl) maybeCreateConnection(addr *core.Address, owner bool) (*ConnectionImpl, error) {
	//// check whether the connection exists before creating it
	//if conn, found := m.connections[addr.String()]; found {
	//	return conn, nil
	//}
	// TODO: check whether we can create a connection
	if conn, err := m.createConnection(addr); err != nil {
		return nil, core.NewHazelcastTargetDisconnectedError(err.Error(), err)
	} else if err = m.authenticate(conn, owner); err != nil {
		return nil, err
	} else {
		m.connectionsMu.Lock()
		defer m.connectionsMu.Unlock()
		m.connections[addr.String()] = conn
		return conn, nil
	}
}

func (m *ConnectionManagerImpl) createConnection(addr *core.Address) (*ConnectionImpl, error) {
	connection := m.createDefaultConnection()
	if socket, err := connection.createSocket(m.networkConfig, addr); err != nil {
		return nil, err
	} else {
		connection.socket = socket
		connection.init()
		connection.clientMessageReader = newClientMessageReader()
		if err := connection.sendProtocolStarter(); err != nil {
			return nil, err
		}
		go connection.writePool()
		go connection.readPool()
		return connection, nil
	}
}

func (m *ConnectionManagerImpl) createDefaultConnection() *ConnectionImpl {
	builder := &clientMessageBuilder{
		handleResponse:     m.invocationService.HandleResponse,
		incompleteMessages: make(map[int64]*proto.ClientMessage),
	}
	return &ConnectionImpl{
		pending:              make(chan *proto.ClientMessage, 1),
		received:             make(chan *proto.ClientMessage, 1),
		closed:               make(chan struct{}),
		clientMessageBuilder: builder,
		readBuffer:           make([]byte, 0),
		connectionID:         m.NextConnectionID(),
		connectionManager:    m,
		status:               0,
		logger:               m.logger,
	}
}

func (cm *ConnectionManagerImpl) authenticate(connection *ConnectionImpl, asOwner bool) error {
	cm.credentials.SetEndpoint(connection.socket.LocalAddr().String())
	request := cm.encodeAuthenticationRequest(asOwner)
	invTimeout := cm.invocationService.InvocationTimeout()
	inv := NewConnectionBoundInvocation(request, -1, nil, connection, invTimeout)
	invocationResult := cm.invocationService.Send(inv)
	result, err := invocationResult.GetWithTimeout(cm.heartbeatTimeout)
	if err != nil {
		return err
	}
	return cm.processAuthenticationResult(connection, asOwner, result)
}

func (cm *ConnectionManagerImpl) processAuthenticationResult(connection *ConnectionImpl, asOwner bool, result *proto.ClientMessage) error {
	status, address, memberUuid, _, serverHazelcastVersion, partitionCount, _, _ := codec.DecodeClientAuthenticationResponse(result)
	switch status {
	case authenticated:
		connection.setConnectedServerVersion(serverHazelcastVersion)
		connection.endpoint.Store(address)
		connection.isOwnerConnection = asOwner
		cm.connections[address.String()] = connection
		// TODO:
		//go cm.fireConnectionAddedEvent(connection)
		if asOwner {
			cm.partitionService.checkAndSetPartitionCount(partitionCount)
			cm.clusterService.ownerConnectionAddr.Store(address)
			cm.clusterService.ownerUUID.Store(memberUuid.String())
			cm.clusterService.uuid.Store(memberUuid.String())
			cm.logger.Info("Setting ", connection, " as owner.")
		}
	case credentialsFailed:
		return core.NewHazelcastAuthenticationError("invalid credentials!", nil)
	case serializationVersionMismatch:
		return core.NewHazelcastAuthenticationError("serialization version mismatches with the server!", nil)
	}
	return nil
}

func (cm *ConnectionManagerImpl) encodeAuthenticationRequest(asOwner bool) *proto.ClientMessage {
	if creds, ok := cm.credentials.(*security.UsernamePasswordCredentials); ok {
		return cm.createAuthenticationRequest(asOwner, creds)
	}
	return cm.createCustomAuthenticationRequest(asOwner)

}

func (cm *ConnectionManagerImpl) createAuthenticationRequest(asOwner bool,
	creds *security.UsernamePasswordCredentials) *proto.ClientMessage {
	//uuid := cm.client.ClusterService.uuid.Load().(string)
	//ownerUUID := cm.client.ClusterService.ownerUUID.Load().(string)
	return codec.EncodeClientAuthenticationRequest(
		"dev",
		"",
		"",
		core.NewUUID(),
		proto.ClientType,
		byte(serializationVersion),
		ClientVersion,
		cm.clientName,
		nil,
	)
}

func (cm *ConnectionManagerImpl) createCustomAuthenticationRequest(asOwner bool) *proto.ClientMessage {
	uuid := cm.clusterService.uuid.Load().(string)
	ownerUUID := cm.clusterService.ownerUUID.Load().(string)
	credsData, err := cm.serializationService.ToData(cm.credentials)
	if err != nil {
		cm.logger.Error("Credentials cannot be serialized!")
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
	address *core.Address,
	uuid core.UUID,
	ownerUuid core.UUID,
	serializationVersion uint8,
	serverHazelcastVersion string,
	partitionCount int32,
	clientUnregisteredMembers []*proto.Member)

func (cm *ConnectionManagerImpl) getAuthenticationDecoder() AuthenticationDecoder {
	var authenticationDecoder AuthenticationDecoder
	if _, ok := cm.credentials.(*security.UsernamePasswordCredentials); ok {
		authenticationDecoder = proto.DecodeClientAuthenticationResponse
	} else {
		// TODO: rename proto.ClientAuthenticationCustomDecodeResponse
		authenticationDecoder = proto.ClientAuthenticationCustomDecodeResponse
	}
	return authenticationDecoder
}

func (m *ConnectionManagerImpl) notifyConnectionClosed(conn *ConnectionImpl, connErr error) {
	if addr, ok := conn.endpoint.Load().(*core.Address); ok {
		// delete authenticated connection
		m.connectionsMu.Lock()
		delete(m.connections, addr.String())
		m.connectionsMu.Unlock()
		listeners := m.copyListeners()
		if len(listeners) > 0 {
			// running listeners in a goroutine in order to protect against long-running listeners
			go m.notifyListenersConnectionClosed(listeners, conn, connErr)
		}
	} else {
		// delete unauthenticated connection
		//m.invocationService.CleanupConnection(conn, connErr)
	}
}

func (m *ConnectionManagerImpl) notifyListenersConnectionClosed(listeners []Listener, conn *ConnectionImpl, connErr error) {
	defer func() {
		if err := recover(); err != nil {
			m.logger.Error("recovered", err)
		}
	}()
	for _, listener := range listeners {
		listener.ConnectionClosed(conn, connErr)
	}
}

func (m *ConnectionManagerImpl) copyListeners() []Listener {
	m.listenersMu.RLock()
	defer m.listenersMu.RUnlock()
	if len(m.listeners) == 0 {
		return nil
	}
	listeners := make([]Listener, len(m.listeners))
	copy(listeners, m.listeners)
	return listeners
}

func checkOwnerConn(owner bool, conn *ConnectionImpl) bool {
	return owner && conn.isOwnerConnection
}
