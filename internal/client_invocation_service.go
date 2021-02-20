package internal

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"sync"
)

type ClientInvocationService interface {
	InvokeOnConnection(clientInvocation ClientInvocation, connection TcpClientConnection) bool
	InvokeOnPartitionOwner(invocation ClientInvocation, partitionId int32) bool
	InvokeOnTarget(invocation ClientInvocation, uuid core.UUID) bool
	Invoke(invocation ClientInvocation) bool
	ClientResponseHandler(response interface{})
	IsSmartRoutingEnabled() bool
}

type clientInvocationServiceImpl struct {
	client *HazelcastClient

	connectionManager ClientConnectionManager
	partitionService  *partitionService

	invocations   map[int64]ClientInvocation
	invocationsMU sync.RWMutex

	isSmartRoutingEnabled bool

	logger logger.Logger
}

func NewClientInvocationService(client *HazelcastClient) ClientInvocationService {
	return &clientInvocationServiceImpl{
		connectionManager:     client.getClientConnectionManager(),
		partitionService:      client.getPartitionService(),
		isSmartRoutingEnabled: client.Config.NetworkConfig().IsSmartRouting(),
	}
}

func (c *clientInvocationServiceImpl) InvokeOnConnection(clientInvocation ClientInvocation, connection TcpClientConnection) bool {
	return c.send(clientInvocation, connection)
}

func (c *clientInvocationServiceImpl) InvokeOnPartitionOwner(invocation ClientInvocation, partitionId int32) bool {
	partitionOwner := c.partitionService.GetPartitionOwner(partitionId)
	if partitionOwner == nil {
		return false
	}
	return c.InvokeOnTarget(invocation, partitionOwner)
}

func (c *clientInvocationServiceImpl) InvokeOnTarget(invocation ClientInvocation, uuid core.UUID) bool {
	connection := c.connectionManager.GetConnection(uuid)
	if connection == nil {
		c.logger.Debug("No connection found to invoke")
		return false
	}

	return c.send(invocation, connection)
}

func (c *clientInvocationServiceImpl) Invoke(invocation ClientInvocation) bool {
	connection := c.connectionManager.GetRandomConnection()
	if connection == nil {
		c.logger.Debug("No connection found to invoke")
		return false
	}

	return c.send(invocation, connection)
}

func (c *clientInvocationServiceImpl) ClientResponseHandler(response interface{}) {
	panic("implement me")
}

func (c *clientInvocationServiceImpl) IsSmartRoutingEnabled() bool {
	return c.isSmartRoutingEnabled
}

func (c *clientInvocationServiceImpl) send(clientInvocation ClientInvocation, connection TcpClientConnection) bool {
	c.registerInvocation(clientInvocation, connection)
	clientMessage := clientInvocation.GetClientMessage()
	if !c.writeToConnection(connection, clientMessage) {
		return false
	}
	clientInvocation.SetSendConnection(connection)
	return true
}

func (c *clientInvocationServiceImpl) writeToConnection(connection TcpClientConnection, message *proto.ClientMessage) bool {
	connection.Write(message)
	return true
}

func (c *clientInvocationServiceImpl) registerInvocation(clientInvocation ClientInvocation, connection TcpClientConnection) {
	clientMessage := clientInvocation.GetClientMessage()
	correlationID := clientMessage.GetCorrelationID()
	c.invocationsMU.Lock()
	c.invocations[correlationID] = clientInvocation
	c.invocationsMU.Unlock()
	/*	handler := clientInvocation.GetEventHandler()
		if handler != nil {
			c.eventHandlersMU.Lock()
			c.eventHandlers[correlationID] = clientInvocation
			c.eventHandlersMU.Unlock()
		}*/
}

type ClientResponseHandler struct {
	clientInvocationService ClientInvocationService
	responseChannel         chan interface{}
}
