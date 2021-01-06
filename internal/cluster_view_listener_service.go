package internal

import (
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"sync"
)

type clusterViewListenerService struct {
	client                  *HazelcastClient
	listenerAddedConnection *Connection
	logger                  logger.Logger
	rwMu                    sync.RWMutex
}

func newClusterViewListenerService(client *HazelcastClient) clusterViewListenerService {
	service := clusterViewListenerService{client: client, logger: client.logger}
	return service
}

func (c *clusterViewListenerService) start() {
	c.client.ConnectionManager.addListener(c)
}

func (c *clusterViewListenerService) onConnectionOpened(connection *Connection) {
	c.tryRegister(connection)
}

func (c *clusterViewListenerService) onConnectionClosed(connection *Connection, cause error) {
	c.tryReRegisterToRandomConnection(connection)
}

func (c *clusterViewListenerService) tryRegister(connection *Connection) {
	c.rwMu.Lock()
	if c.listenerAddedConnection != nil {
		return
	}
	c.listenerAddedConnection = connection
	c.rwMu.Unlock()
	clientMessage := codec.ClientAddClusterViewListenerCodec.EncodeRequest()
	invocation := newInvocation(clientMessage, -1, nil, connection, c.client)
	invocation.eventHandler = func(clientMessage *proto.ClientMessage) {
		codec.ClientAddClusterViewListenerCodec.Handle(clientMessage, c.client.ClusterService.handleMembersViewEvent, nil)
	}
	c.client.ClusterService.clearMemberListVersion()
	message, invocationErr := c.client.InvocationService.sendInvocation(invocation).Result()
	if message != nil {
		c.logger.Trace("ClusterViewListenerService Registered cluster view handler to ", connection)
		return
	}
	if invocationErr != nil {
		c.tryReRegisterToRandomConnection(connection)
	}
}

func (c *clusterViewListenerService) tryReRegisterToRandomConnection(oldConnection *Connection) {
	c.rwMu.Lock()
	if c.listenerAddedConnection != oldConnection {
		return
	}
	c.listenerAddedConnection = nil
	c.rwMu.Unlock()
	newConnection := c.client.ConnectionManager.getRandomConnection()
	if newConnection != nil {
		c.tryRegister(newConnection)
	}
}
