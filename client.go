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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cloud"
	icluster "github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/internal/security"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/stats"
	"github.com/hazelcast/hazelcast-go-client/types"
)

var nextId int32

const (
	created int32 = iota
	starting
	ready
	stopping
	stopped
)

var (
	ErrClientCannotStart = errors.New("client cannot start")
	ErrClientNotActive   = errors.New("client not active")
	ErrContextIsNil      = errors.New("context is nil")
)

// StartNewClient creates and starts a new client.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one or more of the
// cluster members and delegates all cluster wide operations to them.
func StartNewClient() (*Client, error) {
	return StartNewClientWithConfig(NewConfig())
}

// StartNewClientWithConfig creates and starts a new client with the given configuration.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one or more of the
// cluster members and delegates all cluster wide operations to them.
func StartNewClientWithConfig(config Config) (*Client, error) {
	if client, err := newClient(config); err != nil {
		return nil, err
	} else if err = client.start(); err != nil {
		return nil, err
	} else {
		return client, nil
	}
}

type Client struct {
	invocationHandler       invocation.Handler
	logger                  ilogger.Logger
	membershipListenerMapMu *sync.Mutex
	connectionManager       *icluster.ConnectionManager
	clusterService          *icluster.Service
	partitionService        *icluster.PartitionService
	invocationService       *invocation.Service
	serializationService    *serialization.Service
	eventDispatcher         *event.DispatchService
	userEventDispatcher     *event.DispatchService
	proxyManager            *proxyManager
	statsService            *stats.Service
	clusterConfig           *cluster.Config
	membershipListenerMap   map[types.UUID]int64
	refIDGen                *iproxy.ReferenceIDGenerator
	lifecyleListenerMap     map[types.UUID]int64
	lifecyleListenerMapMu   *sync.Mutex
	name                    string
	state                   int32
}

func newClient(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	config = config.Clone()
	id := atomic.AddInt32(&nextId, 1)
	name := ""
	if config.ClientName != "" {
		name = config.ClientName
	}
	if name == "" {
		name = fmt.Sprintf("hz.client_%d", id)
	}
	logLevel, err := ilogger.GetLogLevel(config.LoggerConfig.Level)
	if err != nil {
		return nil, err
	}
	clientLogger := ilogger.NewWithLevel(logLevel)
	// TODO: Move addrProviderTranslator to createComponents
	// Not doing that right now, because of the event dispatchers.
	addrProvider, addrTranslator, err := addrProviderTranslator(context.Background(), &config.ClusterConfig, clientLogger)
	if err != nil {
		return nil, err
	}
	serializationService, err := serialization.NewService(&config.SerializationConfig)
	if err != nil {
		return nil, err
	}
	clientLogger.Trace(func() string { return fmt.Sprintf("creating new client: %s", name) })
	c := &Client{
		name:                    name,
		clusterConfig:           &config.ClusterConfig,
		serializationService:    serializationService,
		eventDispatcher:         event.NewDispatchService(),
		userEventDispatcher:     event.NewDispatchService(),
		logger:                  clientLogger,
		refIDGen:                iproxy.NewReferenceIDGenerator(1),
		lifecyleListenerMap:     map[types.UUID]int64{},
		lifecyleListenerMapMu:   &sync.Mutex{},
		membershipListenerMap:   map[types.UUID]int64{},
		membershipListenerMapMu: &sync.Mutex{},
	}
	c.addConfigEvents(&config)
	c.subscribeUserEvents()
	c.createComponents(&config, addrProvider, addrTranslator)
	return c, nil
}

// Name returns client's name
// Use ConfigBuilder.SetName to set the client name.
// If not set manually, an automatic name is used.
func (c *Client) Name() string {
	return c.name
}

// GetList returns a list instance.
func (c *Client) GetList(ctx context.Context, name string) (*List, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, ErrClientNotActive
	}
	return c.proxyManager.getList(ctx, name)
}

// GetMap returns a distributed map instance.
func (c *Client) GetMap(ctx context.Context, name string) (*Map, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, ErrClientNotActive
	}
	return c.proxyManager.getMap(ctx, name)
}

func (c *Client) GetReplicatedMap(ctx context.Context, name string) (*ReplicatedMap, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, ErrClientNotActive
	}
	return c.proxyManager.getReplicatedMap(ctx, name)
}

// GetQueue returns a queue instance.
func (c *Client) GetQueue(ctx context.Context, name string) (*Queue, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, ErrClientNotActive
	}
	return c.proxyManager.getQueue(ctx, name)
}

// GetTopic returns a topic instance.
func (c *Client) GetTopic(ctx context.Context, name string) (*Topic, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, ErrClientNotActive
	}
	return c.proxyManager.getTopic(ctx, name)
}

// GetSet returns a set instance.
func (c *Client) GetSet(ctx context.Context, name string) (*Set, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, ErrClientNotActive
	}
	return c.proxyManager.getSet(ctx, name)
}

// GetPNCounter returns a PNCounter instance.
func (c *Client) GetPNCounter(ctx context.Context, name string) (*PNCounter, error) {
	if atomic.LoadInt32(&c.state) != ready {
		return nil, ErrClientNotActive
	}
	return c.proxyManager.getPNCounter(ctx, name)
}

// Start connects the client to the cluster.
func (c *Client) start() error {
	if !atomic.CompareAndSwapInt32(&c.state, created, starting) {
		return ErrClientCannotStart
	}
	// TODO: Recover from panics and return as error
	c.eventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateStarting))
	c.clusterService.Start()
	if err := c.connectionManager.Start(1 * time.Minute); err != nil {
		c.clusterService.Stop()
		c.eventDispatcher.Stop()
		c.userEventDispatcher.Stop()
		return err
	}
	if c.statsService != nil {
		c.statsService.Start()
	}
	atomic.StoreInt32(&c.state, ready)
	c.eventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateStarted))
	return nil
}

// Shutdown disconnects the client from the cluster.
func (c *Client) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&c.state, ready, stopping) {
		return ErrClientNotActive
	}
	c.eventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateShuttingDown))
	c.invocationService.Stop()
	c.clusterService.Stop()
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

// Running checks whether or not the client is running.
func (c *Client) Running() bool {
	return atomic.LoadInt32(&c.state) == ready
}

// AddLifecycleListener adds a lifecycle state change handler after the client starts.
// The listener is attached to the client after the client starts, so lifecyle events after the client start can be received.
// Use the returned subscription ID to remove the listener.
// The handler must not block.
func (c *Client) AddLifecycleListener(handler LifecycleStateChangeHandler) (types.UUID, error) {
	if atomic.LoadInt32(&c.state) >= stopping {
		return types.UUID{}, ErrClientNotActive
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
		return ErrClientNotActive
	}
	c.lifecyleListenerMapMu.Lock()
	if intID, ok := c.lifecyleListenerMap[subscriptionID]; ok {
		c.userEventDispatcher.Unsubscribe(eventLifecycleEventStateChanged, intID)
		delete(c.lifecyleListenerMap, subscriptionID)
	}
	c.lifecyleListenerMapMu.Unlock()
	return nil
}

// AddMembershipListener adds a member state change handler with a unique subscription ID.
// The listener is attached to the client after the client starts, so membership events after the client start can be received.
// Use the returned subscription ID to remove the listener.
func (c *Client) AddMembershipListener(handler cluster.MembershipStateChangeHandler) (types.UUID, error) {
	if atomic.LoadInt32(&c.state) >= stopping {
		return types.UUID{}, ErrClientNotActive
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
		return ErrClientNotActive
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

func (c *Client) AddDistributedObjectListener(ctx context.Context, handler DistributedObjectNotifiedHandler) (types.UUID, error) {
	if atomic.LoadInt32(&c.state) >= stopping {
		return types.UUID{}, ErrClientNotActive
	}
	return c.proxyManager.addDistributedObjectEventListener(ctx, handler)
}

func (c *Client) RemoveDistributedObjectListener(ctx context.Context, subscriptionID types.UUID) error {
	if atomic.LoadInt32(&c.state) >= stopping {
		return ErrClientNotActive
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
		c.userEventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateClientConnected))
	})
	c.eventDispatcher.SubscribeSync(icluster.EventDisconnected, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateClientDisconnected))
	})
	c.eventDispatcher.SubscribeSync(icluster.EventMembersAdded, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(event)
	})
	c.eventDispatcher.SubscribeSync(icluster.EventMembersRemoved, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(event)
	})
}

func (c *Client) makeCredentials(config *Config) *security.UsernamePasswordCredentials {
	securityConfig := config.ClusterConfig.SecurityConfig
	return security.NewUsernamePasswordCredentials(securityConfig.Username, securityConfig.Password)
}

func (c *Client) createComponents(config *Config, addrProvider icluster.AddressProvider, addrTranslator icluster.AddressTranslator) {
	credentials := c.makeCredentials(config)
	requestCh := make(chan invocation.Invocation, 1024)
	responseCh := make(chan *proto.ClientMessage, 1024)
	removeCh := make(chan int64, 1024)
	partitionService := icluster.NewPartitionService(icluster.PartitionServiceCreationBundle{
		EventDispatcher: c.eventDispatcher,
		Logger:          c.logger,
	})
	invocationFactory := icluster.NewConnectionInvocationFactory(&config.ClusterConfig)
	clusterService := icluster.NewService(icluster.CreationBundle{
		AddrProvider:      addrProvider,
		RequestCh:         requestCh,
		InvocationFactory: invocationFactory,
		EventDispatcher:   c.eventDispatcher,
		PartitionService:  partitionService,
		Logger:            c.logger,
		Config:            &config.ClusterConfig,
		AddressTranslator: addrTranslator,
	})
	connectionManager := icluster.NewConnectionManager(icluster.ConnectionManagerCreationBundle{
		RequestCh:            requestCh,
		ResponseCh:           responseCh,
		Logger:               c.logger,
		ClusterService:       clusterService,
		PartitionService:     partitionService,
		SerializationService: c.serializationService,
		EventDispatcher:      c.eventDispatcher,
		InvocationFactory:    invocationFactory,
		ClusterConfig:        &config.ClusterConfig,
		Credentials:          credentials,
		ClientName:           c.name,
		AddrTranslator:       addrTranslator,
		Labels:               config.Labels,
	})
	invocationHandler := icluster.NewConnectionInvocationHandler(icluster.ConnectionInvocationHandlerCreationBundle{
		ConnectionManager: connectionManager,
		ClusterService:    clusterService,
		Logger:            c.logger,
		Config:            &config.ClusterConfig,
	})
	invocationService := invocation.NewService(requestCh, responseCh, removeCh, invocationHandler, c.logger)
	listenerBinder := icluster.NewConnectionListenerBinder(
		connectionManager,
		invocationFactory,
		requestCh,
		removeCh,
		c.eventDispatcher,
		c.logger,
		config.ClusterConfig.SmartRouting)
	proxyManagerServiceBundle := creationBundle{
		RequestCh:            requestCh,
		RemoveCh:             removeCh,
		SerializationService: c.serializationService,
		PartitionService:     partitionService,
		ClusterService:       clusterService,
		Config:               config,
		InvocationFactory:    invocationFactory,
		ListenerBinder:       listenerBinder,
		Logger:               c.logger,
	}
	if config.StatsConfig.Enabled {
		c.statsService = stats.NewService(
			requestCh,
			invocationFactory,
			c.eventDispatcher,
			c.logger,
			config.StatsConfig.Period,
			c.name)
	}
	c.connectionManager = connectionManager
	c.clusterService = clusterService
	c.partitionService = partitionService
	c.invocationService = invocationService
	c.proxyManager = newProxyManager(proxyManagerServiceBundle)
	c.invocationHandler = invocationHandler
}

func addrProviderTranslator(ctx context.Context, config *cluster.Config, logger ilogger.Logger) (icluster.AddressProvider, icluster.AddressTranslator, error) {
	if config.HazelcastCloudConfig.Enabled {
		dc := cloud.NewDiscoveryClient(&config.HazelcastCloudConfig, logger)
		nodes, err := dc.DiscoverNodes(ctx)
		if err != nil {
			return nil, nil, err
		}
		pr, err := cloud.NewAddressProvider(dc, nodes)
		if err != nil {
			return nil, nil, err
		}
		return pr, cloud.NewAddressTranslator(dc, nodes), nil
	}
	pr := icluster.NewDefaultAddressProvider(config)
	if config.DiscoveryConfig.UsePublicIP {
		return pr, icluster.NewDefaultPublicAddressTranslator(), nil
	}
	return pr, icluster.NewDefaultAddressTranslator(), nil
}
