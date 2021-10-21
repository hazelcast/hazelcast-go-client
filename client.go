/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hazelcast

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/cloud"
	icluster "github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/stats"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	ClientVersion = internal.ClientVersion
)

var nextId int32

const (
	created int32 = iota
	starting
	ready
	stopping
	stopped
)

// StartNewClient creates and starts a new client with the default configuration.
// The default configuration is tuned connect to an Hazelcast cluster running on the same computer with the client.
func StartNewClient(ctx context.Context) (*Client, error) {
	return StartNewClientWithConfig(ctx, NewConfig())
}

// StartNewClientWithConfig creates and starts a new client with the given configuration.
func StartNewClientWithConfig(ctx context.Context, config Config) (*Client, error) {
	if client, err := newClient(config); err != nil {
		return nil, err
	} else if err = client.start(ctx); err != nil {
		return nil, err
	} else {
		return client, nil
	}
}

// Client enables you to do all Hazelcast operations without being a member of the cluster.
// It connects to one or more of the cluster members and delegates all cluster wide operations to them.
type Client struct {
	invocationHandler       invocation.Handler
	logger                  ilogger.Logger
	membershipListenerMapMu *sync.Mutex
	connectionManager       *icluster.ConnectionManager
	clusterService          *icluster.Service
	partitionService        *icluster.PartitionService
	viewListenerService     *icluster.ViewListenerService
	invocationService       *invocation.Service
	serializationService    *serialization.Service
	eventDispatcher         *event.DispatchService
	userEventDispatcher     *event.DispatchService
	proxyManager            *proxyManager
	statsService            *stats.Service
	heartbeatService        *icluster.HeartbeatService
	clusterConfig           *cluster.Config
	membershipListenerMap   map[types.UUID]int64
	refIDGen                *iproxy.ReferenceIDGenerator
	lifecyleListenerMap     map[types.UUID]int64
	lifecyleListenerMapMu   *sync.Mutex
	name                    string
	state                   int32
}

func newClient(config Config) (*Client, error) {
	config = config.Clone()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	id := atomic.AddInt32(&nextId, 1)
	name := ""
	if config.ClientName != "" {
		name = config.ClientName
	}
	if name == "" {
		name = fmt.Sprintf("hz.client_%d", id)
	}
	logLevel, err := ilogger.GetLogLevel(config.Logger.Level)
	if err != nil {
		return nil, err
	}
	clientLogger := ilogger.NewWithLevel(logLevel)
	serializationService, err := serialization.NewService(&config.Serialization)
	if err != nil {
		return nil, err
	}
	clientLogger.Trace(func() string { return fmt.Sprintf("creating new client: %s", name) })
	c := &Client{
		name:                    name,
		clusterConfig:           &config.Cluster,
		serializationService:    serializationService,
		eventDispatcher:         event.NewDispatchService(clientLogger),
		userEventDispatcher:     event.NewDispatchService(clientLogger),
		logger:                  clientLogger,
		refIDGen:                iproxy.NewReferenceIDGenerator(1),
		lifecyleListenerMap:     map[types.UUID]int64{},
		lifecyleListenerMapMu:   &sync.Mutex{},
		membershipListenerMap:   map[types.UUID]int64{},
		membershipListenerMapMu: &sync.Mutex{},
	}
	c.addConfigEvents(&config)
	c.subscribeUserEvents()
	c.createComponents(&config)
	return c, nil
}

// Name returns client's name
// Use config.Name to set the client name.
// If not set manually, an automatically generated name is used.
func (c *Client) Name() string {
	return c.name
}

// GetList returns a list instance.
func (c *Client) GetList(ctx context.Context, name string) (*List, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getList(ctx, name)
}

// GetMap returns a distributed map instance.
func (c *Client) GetMap(ctx context.Context, name string) (*Map, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getMap(ctx, name)
}

// GetReplicatedMap returns a replicated map instance.
func (c *Client) GetReplicatedMap(ctx context.Context, name string) (*ReplicatedMap, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getReplicatedMap(ctx, name)
}

// GetQueue returns a queue instance.
func (c *Client) GetQueue(ctx context.Context, name string) (*Queue, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getQueue(ctx, name)
}

// GetTopic returns a topic instance.
func (c *Client) GetTopic(ctx context.Context, name string) (*Topic, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getTopic(ctx, name)
}

// GetSet returns a set instance.
func (c *Client) GetSet(ctx context.Context, name string) (*Set, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getSet(ctx, name)
}

// GetPNCounter returns a PNCounter instance.
func (c *Client) GetPNCounter(ctx context.Context, name string) (*PNCounter, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getPNCounter(ctx, name)
}

// GetFlakeIDGenerator returns a FlakeIDGenerator instance.
func (c *Client) GetFlakeIDGenerator(ctx context.Context, name string) (*FlakeIDGenerator, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getFlakeIDGenerator(ctx, name)
}

// GetDistributedObjectsInfo returns the information of all objects created cluster-wide.
func (c *Client) GetDistributedObjectsInfo(ctx context.Context) ([]types.DistributedObjectInfo, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, hzerrors.ErrClientNotActive
	}
	request := codec.EncodeClientGetDistributedObjectsRequest()
	resp, err := c.proxyManager.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	return codec.DecodeClientGetDistributedObjectsResponse(resp), nil
}

func (c *Client) start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&c.state, created, starting) {
		return nil
	}
	c.eventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateStarting))
	if err := c.connectionManager.Start(ctx); err != nil {
		c.eventDispatcher.Stop()
		c.userEventDispatcher.Stop()
		c.invocationService.Stop()
		return err
	}
	c.heartbeatService.Start()
	if c.statsService != nil {
		c.statsService.Start()
	}
	c.eventDispatcher.Subscribe(icluster.EventDisconnected, event.DefaultSubscriptionID, c.clusterDisconnected)
	atomic.StoreInt32(&c.state, ready)
	c.eventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateStarted))
	return nil
}

// Shutdown disconnects the client from the cluster and frees resources allocated by the client.
func (c *Client) Shutdown(ctx context.Context) error {
	// Note that passed context is not used at the moment.
	// In the future, we may need to block during shutdown, which would require a context.
	if !atomic.CompareAndSwapInt32(&c.state, ready, stopping) {
		return nil
	}
	c.eventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateShuttingDown))
	c.invocationService.Stop()
	c.heartbeatService.Stop()
	c.connectionManager.Stop()
	if c.statsService != nil {
		c.statsService.Stop()
	}
	atomic.StoreInt32(&c.state, stopped)
	c.eventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateShutDown))
	// wait for the shut down event to be dispatched
	time.Sleep(1 * time.Millisecond)
	c.eventDispatcher.Stop()
	c.userEventDispatcher.Stop()
	return nil
}

// Running returns true if the client is running.
func (c *Client) Running() bool {
	return atomic.LoadInt32(&c.state) == ready
}

// AddLifecycleListener adds a lifecycle state change handler after the client starts.
// Use the returned subscription ID to remove the listener.
// The handler must not block.
func (c *Client) AddLifecycleListener(handler LifecycleStateChangeHandler) (types.UUID, error) {
	if atomic.LoadInt32(&c.state) >= stopping {
		return types.UUID{}, hzerrors.ErrClientNotActive
	}
	uuid := types.NewUUID()
	subscriptionID := c.refIDGen.NextID()
	c.addLifecycleListener(subscriptionID, handler)
	c.lifecyleListenerMapMu.Lock()
	c.lifecyleListenerMap[uuid] = subscriptionID
	c.lifecyleListenerMapMu.Unlock()
	return uuid, nil
}

// RemoveLifecycleListener removes the lifecycle state change handler with the given subscription ID
func (c *Client) RemoveLifecycleListener(subscriptionID types.UUID) error {
	if atomic.LoadInt32(&c.state) >= stopping {
		return hzerrors.ErrClientNotActive
	}
	c.lifecyleListenerMapMu.Lock()
	if intID, ok := c.lifecyleListenerMap[subscriptionID]; ok {
		c.userEventDispatcher.Unsubscribe(eventLifecycleEventStateChanged, intID)
		delete(c.lifecyleListenerMap, subscriptionID)
	}
	c.lifecyleListenerMapMu.Unlock()
	return nil
}

// AddMembershipListener adds a member state change handler and returns a unique subscription ID.
// Use the returned subscription ID to remove the listener.
func (c *Client) AddMembershipListener(handler cluster.MembershipStateChangeHandler) (types.UUID, error) {
	if atomic.LoadInt32(&c.state) >= stopping {
		return types.UUID{}, hzerrors.ErrClientNotActive
	}
	uuid := types.NewUUID()
	subscriptionID := c.refIDGen.NextID()
	c.addMembershipListener(subscriptionID, handler)
	c.membershipListenerMapMu.Lock()
	c.membershipListenerMap[uuid] = subscriptionID
	c.membershipListenerMapMu.Unlock()
	return uuid, nil
}

// RemoveMembershipListener removes the member state change handler with the given subscription ID.
func (c *Client) RemoveMembershipListener(subscriptionID types.UUID) error {
	if atomic.LoadInt32(&c.state) >= stopping {
		return hzerrors.ErrClientNotActive
	}
	c.membershipListenerMapMu.Lock()
	if intID, ok := c.membershipListenerMap[subscriptionID]; ok {
		c.userEventDispatcher.Unsubscribe(icluster.EventMembersAdded, intID)
		c.userEventDispatcher.Unsubscribe(icluster.EventMembersRemoved, intID)
		delete(c.membershipListenerMap, subscriptionID)
	}
	c.membershipListenerMapMu.Unlock()
	return nil
}

// AddDistributedObjectListener adds a distributed object listener and returns a unique subscription ID.
// Use the returned subscription ID to remove the listener.
func (c *Client) AddDistributedObjectListener(ctx context.Context, handler DistributedObjectNotifiedHandler) (types.UUID, error) {
	if atomic.LoadInt32(&c.state) >= stopping {
		return types.UUID{}, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.addDistributedObjectEventListener(ctx, handler)
}

// RemoveDistributedObjectListener removes the distributed object listener handler with the given subscription ID.
func (c *Client) RemoveDistributedObjectListener(ctx context.Context, subscriptionID types.UUID) error {
	if atomic.LoadInt32(&c.state) >= stopping {
		return hzerrors.ErrClientNotActive
	}
	return c.proxyManager.removeDistributedObjectEventListener(ctx, subscriptionID)
}

func (c *Client) addLifecycleListener(subscriptionID int64, handler LifecycleStateChangeHandler) {
	c.userEventDispatcher.SubscribeSync(eventLifecycleEventStateChanged, subscriptionID, func(event event.Event) {
		if stateChangeEvent, ok := event.(*LifecycleStateChanged); ok {
			handler(*stateChangeEvent)
		} else {
			c.logger.Warnf("cannot cast event to hazelcast.LifecycleStateChanged event")
		}
	})
}

func (c *Client) addMembershipListener(subscriptionID int64, handler cluster.MembershipStateChangeHandler) {
	c.userEventDispatcher.SubscribeSync(icluster.EventMembersAdded, subscriptionID, func(event event.Event) {
		if e, ok := event.(*icluster.MembersAdded); ok {
			for _, member := range e.Members {
				handler(cluster.MembershipStateChanged{
					State:  cluster.MembershipStateAdded,
					Member: member,
				})
			}
		} else {
			c.logger.Warnf("cannot cast event to cluster.MembershipStateChanged event")
		}
	})
	c.userEventDispatcher.SubscribeSync(icluster.EventMembersRemoved, subscriptionID, func(event event.Event) {
		if e, ok := event.(*icluster.MembersRemoved); ok {
			for _, member := range e.Members {
				handler(cluster.MembershipStateChanged{
					State:  cluster.MembershipStateRemoved,
					Member: member,
				})
			}
		} else {
			c.logger.Errorf("cannot cast event to cluster.MembersRemoved event")
		}
	})
}

func (c *Client) addConfigEvents(config *Config) {
	for uuid, handler := range config.lifecycleListeners {
		subscriptionID := c.refIDGen.NextID()
		c.addLifecycleListener(subscriptionID, handler)
		c.lifecyleListenerMap[uuid] = subscriptionID
	}
	for uuid, handler := range config.membershipListeners {
		subscriptionID := c.refIDGen.NextID()
		c.addMembershipListener(subscriptionID, handler)
		c.membershipListenerMap[uuid] = subscriptionID
	}
}

func (c *Client) subscribeUserEvents() {
	c.eventDispatcher.SubscribeSync(eventLifecycleEventStateChanged, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(event)
	})
	c.eventDispatcher.SubscribeSync(icluster.EventConnected, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateConnected))
	})
	c.eventDispatcher.SubscribeSync(icluster.EventDisconnected, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateDisconnected))
	})
	c.eventDispatcher.SubscribeSync(icluster.EventChangedCluster, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateChangedCluster))
	})
	c.eventDispatcher.SubscribeSync(icluster.EventMembersAdded, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(event)
	})
	c.eventDispatcher.SubscribeSync(icluster.EventMembersRemoved, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(event)
	})
}

func (c *Client) createComponents(config *Config) {
	partitionService := icluster.NewPartitionService(icluster.PartitionServiceCreationBundle{
		EventDispatcher: c.eventDispatcher,
		Logger:          c.logger,
	})
	invocationFactory := icluster.NewConnectionInvocationFactory(&config.Cluster)
	var failoverConfigs []cluster.Config
	maxTryCount := math.MaxInt32
	if config.Failover.Enabled {
		maxTryCount = config.Failover.TryCount
		failoverConfigs = config.Failover.Configs
	}
	failoverService := icluster.NewFailoverService(c.logger,
		maxTryCount, config.Cluster, failoverConfigs, addrProviderTranslator)
	clusterService := icluster.NewService(icluster.CreationBundle{
		InvocationFactory: invocationFactory,
		EventDispatcher:   c.eventDispatcher,
		PartitionService:  partitionService,
		Logger:            c.logger,
		Config:            &config.Cluster,
		FailoverService:   failoverService,
	})
	connectionManager := icluster.NewConnectionManager(icluster.ConnectionManagerCreationBundle{
		Logger:               c.logger,
		ClusterService:       clusterService,
		PartitionService:     partitionService,
		SerializationService: c.serializationService,
		EventDispatcher:      c.eventDispatcher,
		InvocationFactory:    invocationFactory,
		ClusterConfig:        &config.Cluster,
		ClientName:           c.name,
		FailoverService:      failoverService,
		FailoverConfig:       &config.Failover,
		Labels:               config.Labels,
	})
	viewListener := icluster.NewViewListenerService(clusterService, connectionManager, c.eventDispatcher, c.logger)
	invocationHandler := icluster.NewConnectionInvocationHandler(icluster.ConnectionInvocationHandlerCreationBundle{
		ConnectionManager: connectionManager,
		ClusterService:    clusterService,
		Logger:            c.logger,
		Config:            &config.Cluster,
	})
	invocationService := invocation.NewService(invocationHandler, c.logger)
	listenerBinder := icluster.NewConnectionListenerBinder(
		connectionManager,
		invocationService,
		invocationFactory,
		c.eventDispatcher,
		c.logger,
		!config.Cluster.Unisocket)
	proxyManagerServiceBundle := creationBundle{
		InvocationService:    invocationService,
		SerializationService: c.serializationService,
		PartitionService:     partitionService,
		ClusterService:       clusterService,
		Config:               config,
		InvocationFactory:    invocationFactory,
		ListenerBinder:       listenerBinder,
		Logger:               c.logger,
	}
	c.heartbeatService = icluster.NewHeartbeatService(connectionManager, invocationFactory, invocationService, c.logger)
	if config.Stats.Enabled {
		c.statsService = stats.NewService(
			invocationService,
			invocationFactory,
			c.eventDispatcher,
			c.logger,
			time.Duration(config.Stats.Period),
			c.name)
	}
	c.connectionManager = connectionManager
	c.clusterService = clusterService
	c.partitionService = partitionService
	c.invocationService = invocationService
	c.proxyManager = newProxyManager(proxyManagerServiceBundle)
	c.invocationHandler = invocationHandler
	c.viewListenerService = viewListener
	c.connectionManager.SetInvocationService(invocationService)
	c.clusterService.SetInvocationService(invocationService)
}

func (c *Client) clusterDisconnected(e event.Event) {
	if atomic.LoadInt32(&c.state) != ready {
		return
	}
	ctx := context.Background()
	if c.clusterConfig.ConnectionStrategy.ReconnectMode == cluster.ReconnectModeOff {
		c.logger.Debug(func() string { return "reconnect mode is off, shutting down" })
		c.Shutdown(ctx)
		return
	}
	c.logger.Debug(func() string { return "cluster disconnected, rebooting" })
	// try to reboot cluster connection
	c.connectionManager.Stop()
	c.clusterService.Reset()
	c.partitionService.Reset()
	if err := c.connectionManager.Start(ctx); err != nil {
		c.logger.Errorf("cannot reconnect to cluster, shutting down: %w", err)
		c.Shutdown(ctx)
	}
}

func addrProviderTranslator(config *cluster.Config, logger ilogger.Logger) (icluster.AddressProvider, icluster.AddressTranslator) {
	if config.Cloud.Enabled {
		dc := cloud.NewDiscoveryClient(&config.Cloud, logger)
		return cloud.NewAddressProvider(dc), cloud.NewAddressTranslator(dc)
	}
	pr := icluster.NewDefaultAddressProvider(&config.Network)
	if config.Discovery.UsePublicIP {
		return pr, icluster.NewDefaultPublicAddressTranslator()
	}
	return pr, icluster.NewDefaultAddressTranslator()
}
