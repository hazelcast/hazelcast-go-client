package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
	"sync"
)

type registration struct {
	client int
	server internal.UUID
}

type connRegistration struct {
	conn   *ConnectionImpl
	client int
	server internal.UUID
}

type ConnectionListenerBinderImpl struct {
	connectionManager *ConnectionManagerImpl
	requestCh         chan<- invocation.Invocation
	// connectionID -> clientRegistrationID -> serverRegistrationID
	connToRegistration   map[int64]map[int]internal.UUID
	connToRegistrationMu *sync.Mutex
}

func NewConnectionListenerBinderImpl(
	connManager *ConnectionManagerImpl,
	requestCh chan<- invocation.Invocation) *ConnectionListenerBinderImpl {
	return &ConnectionListenerBinderImpl{
		connectionManager:    connManager,
		requestCh:            requestCh,
		connToRegistration:   map[int64]map[int]internal.UUID{},
		connToRegistrationMu: &sync.Mutex{},
	}
}

func (b *ConnectionListenerBinderImpl) Add(
	request *proto.ClientMessage,
	clientRegistrationID int,
	handler proto.ClientMessageHandler) error {
	connToRegistration := map[int64]registration{}
	for _, conn := range b.connectionManager.GetActiveConnections() {
		inv := NewConnectionBoundInvocation(
			request,
			-1,
			nil,
			conn,
			b.connectionManager.invocationTimeout)
		inv.SetEventHandler(handler)
		b.requestCh <- inv
		if response, err := inv.Get(); err != nil {
			return err
		} else {
			serverRegistrationID := codec.DecodeMapAddEntryListenerResponse(response)
			connToRegistration[conn.connectionID] = registration{
				client: clientRegistrationID,
				server: serverRegistrationID,
			}
		}
	}
	// merge connToRegistration to the main one
	b.connToRegistrationMu.Lock()
	defer b.connToRegistrationMu.Unlock()
	for connID, reg := range connToRegistration {
		if connReg, ok := b.connToRegistration[connID]; ok {
			connReg[reg.client] = reg.server
		} else {
			b.connToRegistration[connID] = map[int]internal.UUID{
				reg.client: reg.server,
			}
		}
	}
	return nil
}

func (b *ConnectionListenerBinderImpl) Remove(
	mapName string,
	clientRegistrationID int) error {
	activeConnections := b.connectionManager.GetActiveConnections()
	connToRegistration := []connRegistration{}
	b.connToRegistrationMu.Lock()
	for _, conn := range activeConnections {
		if regs, ok := b.connToRegistration[conn.connectionID]; ok {
			if serverRegistrationID, ok := regs[clientRegistrationID]; ok {
				connToRegistration = append(connToRegistration, connRegistration{
					conn:   conn,
					client: clientRegistrationID,
					server: serverRegistrationID,
				})
			}
		}
	}
	b.connToRegistrationMu.Unlock()
	for _, reg := range connToRegistration {
		request := codec.EncodeMapRemoveEntryListenerRequest(mapName, reg.server)
		inv := NewConnectionBoundInvocation(
			request,
			-1,
			nil,
			reg.conn,
			b.connectionManager.invocationTimeout)
		b.requestCh <- inv
		if _, err := inv.Get(); err != nil {
			return err
		}
	}
	b.connToRegistrationMu.Lock()
	defer b.connToRegistrationMu.Unlock()
	for _, reg := range connToRegistration {
		if regs, ok := b.connToRegistration[reg.conn.connectionID]; ok {
			delete(regs, reg.client)
		}
	}
	return nil
}
