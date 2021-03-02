package connection

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"sync"
	"sync/atomic"
)

type Manager interface {
	AddListener(listener Listener)
	NextConnectionID() int64
	ConnectionForAddress(addr *core.Address) (*Impl, error)
	notifyConnectionClosed(connection *Impl, cause error)
}

type ManagerImpl struct {
	connectionsMu *sync.RWMutex
	connections   map[string]*Impl
	listenersMu   *sync.RWMutex
	listeners     []Listener
	//invocationService invocation.Service
	nextConnectionID  int64
	addressTranslator internal.AddressTranslator
	smartRouting      bool
	alive             atomic.Value
	logger            logger.Logger
}

func NewManagerImpl(bundle CreationBundle) *ManagerImpl {
	bundle.Check()
	manager := &ManagerImpl{
		connectionsMu: &sync.RWMutex{},
		connections:   map[string]*Impl{},
		listenersMu:   &sync.RWMutex{},
		//invocationService: bundle.InvocationService,
		addressTranslator: bundle.AddressTranslator,
		smartRouting:      bundle.SmartRouting,
		logger:            bundle.Logger,
	}
	manager.alive.Store(true)
	return manager
}

func (m *ManagerImpl) AddListener(listener Listener) {
	m.listenersMu.Lock()
	defer m.listenersMu.Unlock()
	m.listeners = append(m.listeners, listener)
}

func (m *ManagerImpl) NextConnectionID() int64 {
	return atomic.AddInt64(&m.nextConnectionID, 1)
}

func (m *ManagerImpl) ConnectionForAddress(addr *core.Address) (*Impl, error) {
	panic("implement me!")
}

func (m *ManagerImpl) notifyConnectionClosed(conn *Impl, connErr error) {
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

func (m *ManagerImpl) notifyListenersConnectionClosed(listeners []Listener, conn *Impl, connErr error) {
	defer func() {
		if err := recover(); err != nil {
			m.logger.Error("recovered", err)
		}
	}()
	for _, listener := range listeners {
		listener.ConnectionClosed(conn, connErr)
	}
}

func (m *ManagerImpl) copyListeners() []Listener {
	m.listenersMu.RLock()
	defer m.listenersMu.RUnlock()
	if len(m.listeners) == 0 {
		return nil
	}
	listeners := make([]Listener, len(m.listeners))
	copy(listeners, m.listeners)
	return listeners
}
