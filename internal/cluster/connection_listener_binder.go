package cluster

import (
	"sync"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

type connRegistration struct {
	conn    *Connection
	client  int
	server  internal.UUID
	request *proto.ClientMessage
}

type ConnectionListenerBinderImpl struct {
	connectionManager    *ConnectionManager
	invocationFactory    *ConnectionInvocationFactory
	requestCh            chan<- invocation.Invocation
	connToRegistration   map[int64]map[int]internal.UUID
	connToRegistrationMu *sync.Mutex
}

func NewConnectionListenerBinderImpl(
	connManager *ConnectionManager,
	invocationFactory *ConnectionInvocationFactory,
	requestCh chan<- invocation.Invocation) *ConnectionListenerBinderImpl {
	return &ConnectionListenerBinderImpl{
		connectionManager:    connManager,
		invocationFactory:    invocationFactory,
		requestCh:            requestCh,
		connToRegistration:   map[int64]map[int]internal.UUID{},
		connToRegistrationMu: &sync.Mutex{},
	}
}

func (b *ConnectionListenerBinderImpl) Add(
	request *proto.ClientMessage,
	clientRegistrationID int,
	handler proto.ClientMessageHandler) error {
	connToRegistration := map[int64]connRegistration{}
	for _, conn := range b.connectionManager.ActiveConnections() {
		inv := b.invocationFactory.NewConnectionBoundInvocation(
			request,
			-1,
			nil,
			conn,
			b.connectionManager.clusterConfig.InvocationTimeout,
			nil)
		inv.SetEventHandler(handler)
		b.requestCh <- inv
		if response, err := inv.Get(); err != nil {
			return err
		} else {
			// TODO: Instead of using DecodeMapAddEntryListenerResponse use the appropriate Decoder
			// Currently all such decoders decode the same value.
			serverRegistrationID := codec.DecodeMapAddEntryListenerResponse(response)
			connToRegistration[conn.connectionID] = connRegistration{
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
	activeConnections := b.connectionManager.ActiveConnections()
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
		inv := b.invocationFactory.NewConnectionBoundInvocation(
			request,
			-1,
			nil,
			reg.conn,
			b.connectionManager.clusterConfig.InvocationTimeout,
			nil)
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
