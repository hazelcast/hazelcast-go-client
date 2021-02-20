package internal

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
	"sync/atomic"
)

type clusterViewListenerService struct {
	client                  *HazelcastClient
	listenerAddedConnection atomic.Value
	logger                  logger.Logger
}

func newClusterViewListenerService(client *HazelcastClient) clusterViewListenerService {
	return clusterViewListenerService{client: client, logger: client.logger}
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
	listenerConnection := c.listenerAddedConnection.Load()
	if listenerConnection != nil {
		return
	}
	c.listenerAddedConnection.Store(connection)

	clientMessage := codec.ClientAddClusterViewListenerCodec.EncodeRequest()
	invocation := newInvocation(clientMessage, -1, nil, connection, c.client)
	invocation.eventHandler = func(clientMessage *proto.ClientMessage) {
		codec.ClientAddClusterViewListenerCodec.Handle(clientMessage, c.client.ClusterService.handleMembersViewEvent, func(version int32, partitions []proto.Pair) {
			c.client.PartitionService.handlePartitionsViewEvent(connection, version, partitions)
		})
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
	listenerConnection := c.listenerAddedConnection.Load()
	if listenerConnection == nil {
		return
	}
	if listenerConnection.(*Connection) != oldConnection {
		return
	}
	c.listenerAddedConnection = atomic.Value{}
	newConnection := c.client.ConnectionManager.getRandomConnection()
	if newConnection != nil {
		c.tryRegister(newConnection)
	}
}
