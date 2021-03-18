package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

type ConnectionListenerBinderImpl struct {
	connectionManager *ConnectionManagerImpl
	requestCh         chan<- invocation.Invocation
}

func NewConnectionListenerBinderImpl(connManager *ConnectionManagerImpl, requestCh chan<- invocation.Invocation) *ConnectionListenerBinderImpl {
	return &ConnectionListenerBinderImpl{
		connectionManager: connManager,
		requestCh:         requestCh,
	}
}

func (b ConnectionListenerBinderImpl) Add(request *proto.ClientMessage, handler proto.ClientMessageHandler) error {
	for _, conn := range b.connectionManager.GetActiveConnections() {
		inv := NewConnectionBoundInvocation(request, -1, nil, conn, b.connectionManager.invocationTimeout)
		inv.SetEventHandler(handler)
		b.requestCh <- inv
		if _, err := inv.Get(); err != nil {
			return err
		}
		// TODO: handle msg
	}
	return nil
}
