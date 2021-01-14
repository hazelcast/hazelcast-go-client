package internal

import (
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"sync"
	"sync/atomic"
	"time"
)

type ClientConnectionManager interface {
	IsAlive() bool

	GetConnection(uuid core.UUID) *Connection

	CheckIfInvocationAllowed() error

	GetClientUUID() core.UUID

	GetRandomConnection() *Connection

	GetActiveConnections() []*Connection

	AddConnectionListener(listener connectionListener)
}

type clientConnectionManager struct {
	client                  *HazelcastClient
	connectionListeners     []connectionListener
	addressTranslator       AddressTranslator
	connectionListenersMU   sync.RWMutex
	loadBalancer            core.LoadBalancer
	activeConnections       sync.Map
	labels                  []string
	logger                  logger.Logger
	connectionTimeoutMillis int32
	heartbeatManager        *heartBeatService
	authenticationTimeout   time.Duration
	clientUUID              core.UUID
	shuffleMemberList       bool
	smartRoutingEnabled     bool
	asyncStart              bool
	isAlive                 atomic.Value
}

func newClientConnectionManager(client *HazelcastClient, addressTranslator AddressTranslator) ClientConnectionManager {
	isAlive := atomic.Value{}
	isAlive.Store(false)

	return &clientConnectionManager{
		client:                  client,
		connectionListeners:     make([]connectionListener, 0),
		addressTranslator:       addressTranslator,
		loadBalancer:            client.Config.LoadBalancer(),
		labels:                  client.Config.GetLabels(),
		logger:                  client.logger,
		connectionTimeoutMillis: initConnectionTimeoutMillis(),
		heartbeatManager:        client.HeartBeatService,
		authenticationTimeout:   client.HeartBeatService.heartBeatTimeout,
		clientUUID:              core.NewUUID(),
		shuffleMemberList:       client.properties.GetBoolean(property.StatisticsEnabled),
		smartRoutingEnabled:     client.Config.NetworkConfig().IsSmartRouting(),
		isAlive:                 isAlive,
	}
}

func initConnectionTimeoutMillis() int32 {
	return 0
}

func (c *clientConnectionManager) Start() {
	if !c.IsAlive() {
		return
	}
	c.heartbeatManager.start()
	c.connectToCluster()
}

func (c *clientConnectionManager) connectToCluster() {
	if c.asyncStart {
		c.submitConnectToClusterTask()
	} else {
		c.doConnectToCluster()
	}
}

func (c *clientConnectionManager) submitConnectToClusterTask() {
	//TODO implement it
	panic("implement me")
}

func (c *clientConnectionManager) doConnectToCluster() {
	c.client.ClusterService.connectToCluster()
}

func (c *clientConnectionManager) connectToAllClusterMembers() {
	for _, eachMember := range c.client.ClusterService.GetMemberList() {
		c.getOrConnectToMember(eachMember)
	}
}

func (c *clientConnectionManager) getOrConnectToMember(member core.Member) *Connection {
	uuid := member.UUID()
	connectionLoad, connectionLoadOk := c.activeConnections.Load(uuid.ToString())
	if connectionLoadOk {
		return connectionLoad.(*Connection)
	}

	address := member.Address()
	address = c.addressTranslator.Translate(address)

	return nil
}

func (c *clientConnectionManager) IsAlive() bool {
	return c.isAlive.Load().(bool)
}

func (c *clientConnectionManager) GetConnection(uuid core.UUID) *Connection {
	connection, _ := c.activeConnections.Load(uuid.ToString())
	return connection.(*Connection)
}

func (c *clientConnectionManager) CheckIfInvocationAllowed() error {
	panic("implement me")
}

func (c *clientConnectionManager) GetClientUUID() core.UUID {
	return c.clientUUID
}

func (c *clientConnectionManager) GetRandomConnection() *Connection {
	if c.smartRoutingEnabled {
		member := c.loadBalancer.Next()
		if member != nil {
			return c.GetConnection(member.UUID())
		}
	}

	for _, connection := range c.GetActiveConnections() {
		return connection
	}

	return nil
}

func (c *clientConnectionManager) GetActiveConnections() []*Connection {
	connections := make([]*Connection, 0)
	c.activeConnections.Range(func(key, value interface{}) bool {
		connections = append(connections, value.(*Connection))
		return true
	})
	return connections
}

func (c *clientConnectionManager) AddConnectionListener(listener connectionListener) {
	c.connectionListenersMU.Lock()
	defer c.connectionListenersMU.Unlock()
	c.connectionListeners = append(c.connectionListeners, listener)
}
