// Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hazelcast

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	icluster "github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/internal/security"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

var nextId int32

type clientState int

const (
	created clientState = iota
	ready
	stopping
	stopped
)

var ErrClientCannotStart = errors.New("client cannot start")
var ErrClientNotReady = errors.New("client not ready")

// StartNewClient creates and starts a new client.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one or more of the
// cluster members and delegates all cluster wide operations to them.
func StartNewClient() (*Client, error) {
	if client, err := NewClient(); err != nil {
		return nil, err
	} else if err = client.Start(); err != nil {
		return nil, err
	} else {
		return client, nil
	}
}

// StartNewClientWithConfig creates and starts a new client with the given configuration.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one or more of the
// cluster members and delegates all cluster wide operations to them.
func StartNewClientWithConfig(configProvider ConfigProvider) (*Client, error) {
	if client, err := NewClientWithConfig(configProvider); err != nil {
		return nil, err
	} else if err = client.Start(); err != nil {
		return nil, err
	} else {
		return client, nil
	}
}

// NewClient creates and returns a new client.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one or more of the
// cluster members and delegates all cluster wide operations to them.
func NewClient() (*Client, error) {
	return NewClientWithConfig(NewConfigBuilder())
}

// NewClientWithConfig creates and returns a new client with the given config.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one or more of the
// cluster members and delegates all cluster wide operations to them.
func NewClientWithConfig(configProvider ConfigProvider) (*Client, error) {
	if config, err := configProvider.Config(); err != nil {
		return nil, err
	} else {
		return newClient("", *config)
	}
}

type Client struct {
	// configuration
	name          string
	clusterConfig *cluster.Config

	// components
	proxyManager        *proxyManager
	connectionManager   *icluster.ConnectionManager
	clusterService      *icluster.ServiceImpl
	partitionService    *icluster.PartitionService
	eventDispatcher     *event.DispatchService
	userEventDispatcher *event.DispatchService
	invocationHandler   invocation.Handler
	logger              logger.Logger

	// state
	state atomic.Value
}

func newClient(name string, config Config) (*Client, error) {
	config = config.clone()
	id := atomic.AddInt32(&nextId, 1)
	if name == "" {
		name = fmt.Sprintf("hz.client_%d", id)
	}
	// TODO: consider disabling manual client name
	if config.ClientName != "" {
		name = config.ClientName
	}
	logLevel, err := logger.GetLogLevel(config.LoggerConfig.Level)
	if err != nil {
		return nil, err
	}
	clientLogger := logger.NewWithLevel(logLevel)
	client := &Client{
		name:                name,
		clusterConfig:       &config.ClusterConfig,
		eventDispatcher:     event.NewDispatchService(),
		userEventDispatcher: event.NewDispatchService(),
		logger:              clientLogger,
	}
	client.state.Store(created)
	client.subscribeUserEvents()
	client.createComponents(&config)
	return client, nil
}

// Name returns client's name
// Use ConfigBuilder.SetName to set the client name.
// If not set manually, an automatic name is used.
func (c *Client) Name() string {
	return c.name
}

// GetMap returns a distributed map instance.
func (c *Client) GetMap(name string) (*Map, error) {
	if !c.ready() {
		return nil, ErrClientNotReady
	}
	return c.proxyManager.GetMap(name)
}

/*
// GetReplicatedMap returns a replicated map instance.
func (c *Client) GetReplicatedMap(name string) (hztypes.ReplicatedMap, error) {
	if !c.ready() {
		return nil, ErrClientNotReady
	}
	m, err := c.proxyManager.GetReplicatedMap(name)
	return m.(hztypes.ReplicatedMap), err
}
*/

// Start connects the client to the cluster.
func (c *Client) Start() error {
	if !c.canStart() {
		return ErrClientCannotStart
	}
	// TODO: Recover from panics and return as error
	c.eventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateStarting))
	clusterServiceStartCh := c.clusterService.Start(c.clusterConfig.SmartRouting)
	c.partitionService.Start()
	if err := c.connectionManager.Start(); err != nil {
		return err
	}
	<-clusterServiceStartCh
	c.state.Store(ready)
	c.eventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateStarted))
	return nil
}

// Shutdown disconnects the client from the cluster.
func (c *Client) Shutdown() error {
	if !c.ready() {
		return ErrClientNotReady
	}
	c.state.Store(stopping)
	c.eventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateShuttingDown))
	c.clusterService.Stop()
	c.partitionService.Stop()
	<-c.connectionManager.Stop()
	c.state.Store(stopped)
	c.eventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateShutDown))
	return nil
}

// ListenLifecycleStateChange adds a lifecycle state change handler with a unique subscription ID.
// The handler must not block.
func (c *Client) ListenLifecycleStateChange(subscriptionID int, handler LifecycleStateChangeHandler) error {
	if !c.canStartOrReady() {
		return ErrClientNotReady
	}
	c.userEventDispatcher.SubscribeSync(EventLifecycleEventStateChanged, subscriptionID, func(event event.Event) {
		if stateChangeEvent, ok := event.(*LifecycleStateChanged); ok {
			handler(*stateChangeEvent)
		} else {
			c.logger.Errorf("cannot cast event to lifecycle.LifecycleStateChanged event")
		}
	})
	return nil
}

// UnlistenLifecycleStateChange removes the lifecycle state change handler with the given subscription ID
func (c *Client) UnlistenLifecycleStateChange(subscriptionID int) error {
	if !c.canStartOrReady() {
		return ErrClientNotReady
	}
	c.userEventDispatcher.Unsubscribe(EventLifecycleEventStateChanged, subscriptionID)
	return nil
}

// ListenMembershipStateChange adds a member state change handler with a unique subscription ID.
func (c *Client) ListenMembershipStateChange(subscriptionID int, handler cluster.MembershipStateChangedHandler) error {
	if !c.canStartOrReady() {
		return ErrClientNotReady
	}
	c.userEventDispatcher.Subscribe(icluster.EventMembersAdded, subscriptionID, func(event event.Event) {
		if membersAddedEvent, ok := event.(*icluster.MembersAdded); ok {
			for _, member := range membersAddedEvent.Members {
				handler(cluster.MembershipStateChanged{
					State:  cluster.MembershipStateAdded,
					Member: member,
				})
			}
		} else {
			c.logger.Errorf("cannot cast event to cluster.MembersAdded event")
		}
	})
	c.userEventDispatcher.Subscribe(icluster.EventMembersRemoved, subscriptionID, func(event event.Event) {
		if membersRemovedEvent, ok := event.(*icluster.MembersRemoved); ok {
			for _, member := range membersRemovedEvent.Members {
				handler(cluster.MembershipStateChanged{
					State:  cluster.MembershipStateRemoved,
					Member: member,
				})
			}
		} else {
			c.logger.Errorf("cannot cast event to cluster.MembersRemoved event")
		}
	})
	return nil
}

// UnlistenMembershipStateChange removes the member state change handler with the given subscription ID.
func (c *Client) UnlistenMembershipStateChange(subscriptionID int) error {
	if !c.canStartOrReady() {
		return ErrClientNotReady
	}
	c.userEventDispatcher.Unsubscribe(icluster.EventMembersAdded, subscriptionID)
	c.userEventDispatcher.Unsubscribe(icluster.EventMembersRemoved, subscriptionID)
	return nil
}

func (c *Client) canStart() bool {
	return c.state.Load() == created
}

func (c *Client) ready() bool {
	return c.state.Load() == ready
}

func (c *Client) canStartOrReady() bool {
	state := c.state.Load()
	return state == created || state == ready
}

func (c *Client) subscribeUserEvents() {
	c.eventDispatcher.SubscribeSync(EventLifecycleEventStateChanged, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(event)
	})
	c.eventDispatcher.Subscribe(icluster.EventConnected, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateClientConnected))
	})
	c.eventDispatcher.Subscribe(icluster.EventDisconnected, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(newLifecycleStateChanged(LifecycleStateClientDisconnected))
	})
	c.eventDispatcher.Subscribe(icluster.EventMembersAdded, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(event)
	})
	c.eventDispatcher.Subscribe(icluster.EventMembersRemoved, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(event)
	})
}

func (c *Client) createComponents(config *Config) {
	credentials := security.NewUsernamePasswordCredentials("dev", "dev-pass")
	serializationService, err := serialization.NewService(&config.SerializationConfig)
	if err != nil {
		panic(fmt.Errorf("error creating client: %w", err))
	}
	smartRouting := config.ClusterConfig.SmartRouting
	addressProviders := []icluster.AddressProvider{
		icluster.NewDefaultAddressProvider(&config.ClusterConfig),
	}
	requestCh := make(chan invocation.Invocation, 1)
	partitionService := icluster.NewPartitionService(icluster.PartitionServiceCreationBundle{
		SerializationService: serializationService,
		EventDispatcher:      c.eventDispatcher,
		Logger:               c.logger,
	})
	invocationFactory := icluster.NewConnectionInvocationFactory(120 * time.Second)
	clusterService := icluster.NewServiceImpl(icluster.CreationBundle{
		AddrProviders:     addressProviders,
		RequestCh:         requestCh,
		InvocationFactory: invocationFactory,
		EventDispatcher:   c.eventDispatcher,
		Logger:            c.logger,
		Config:            &config.ClusterConfig,
	})
	responseCh := make(chan *proto.ClientMessage, 1)
	invocationService := invocation.NewServiceImpl(invocation.ServiceCreationBundle{
		RequestCh:    requestCh,
		ResponseCh:   responseCh,
		SmartRouting: smartRouting,
		Logger:       c.logger,
	})
	connectionManager := icluster.NewConnectionManager(icluster.ConnectionManagerCreationBundle{
		RequestCh:            requestCh,
		ResponseCh:           responseCh,
		SmartRouting:         smartRouting,
		Logger:               c.logger,
		ClusterService:       clusterService,
		PartitionService:     partitionService,
		SerializationService: serializationService,
		EventDispatcher:      c.eventDispatcher,
		InvocationFactory:    invocationFactory,
		ClusterConfig:        &config.ClusterConfig,
		Credentials:          credentials,
		ClientName:           c.name,
	})
	invocationHandler := icluster.NewConnectionInvocationHandler(icluster.ConnectionInvocationHandlerCreationBundle{
		ConnectionManager: connectionManager,
		ClusterService:    clusterService,
		Logger:            c.logger,
	})
	invocationService.SetHandler(invocationHandler)
	listenerBinder := icluster.NewConnectionListenerBinderImpl(connectionManager, invocationFactory, requestCh, c.eventDispatcher)
	proxyManagerServiceBundle := proxy.CreationBundle{
		RequestCh:            requestCh,
		SerializationService: serializationService,
		PartitionService:     partitionService,
		ClusterService:       clusterService,
		SmartRouting:         smartRouting,
		InvocationFactory:    invocationFactory,
		UserEventDispatcher:  c.userEventDispatcher,
		ListenerBinder:       listenerBinder,
		Logger:               c.logger,
	}
	c.connectionManager = connectionManager
	c.clusterService = clusterService
	c.partitionService = partitionService
	c.proxyManager = newManager(proxyManagerServiceBundle)
	c.invocationHandler = invocationHandler
}
