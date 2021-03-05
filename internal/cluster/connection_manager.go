package cluster

import (
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"sync"
	"sync/atomic"
)

type ConnectionManager interface {
	AddListener(listener Listener)
	NextConnectionID() int64
	GetConnectionForAddress(addr *core.Address) *ConnectionImpl
	Start()
	notifyConnectionClosed(connection *ConnectionImpl, cause error)
}

type ConnectionManagerCreationBundle struct {
	//InvocationService invocation.Service
	SmartRouting      bool
	Logger            logger.Logger
	AddressTranslator internal.AddressTranslator
	ClusterService    *ServiceImpl
}

func (b ConnectionManagerCreationBundle) Check() {
	//if b.InvocationService == nil {
	//	panic("InvocationService is nil")
	//}
	if b.Logger == nil {
		panic("Logger is nil")
	}
	if b.AddressTranslator == nil {
		panic("AddressTranslator is nil")
	}
	if b.ClusterService == nil {
		panic("ClusterService is nil")
	}
}

type ConnectionManagerImpl struct {
	connectionsMu *sync.RWMutex
	connections   map[string]*ConnectionImpl
	listenersMu   *sync.RWMutex
	listeners     []Listener
	//invocationService invocation.Service
	// TODO: depend on the interface
	clusterService    *ServiceImpl
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
		connectionsMu: &sync.RWMutex{},
		connections:   map[string]*ConnectionImpl{},
		listenersMu:   &sync.RWMutex{},
		//invocationService: bundle.InvocationService,
		addressTranslator: bundle.AddressTranslator,
		smartRouting:      bundle.SmartRouting,
		logger:            bundle.Logger,
		clusterService:    bundle.ClusterService,
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
