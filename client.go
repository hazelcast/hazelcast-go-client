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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hztypes"
	icluster "github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilifecycle "github.com/hazelcast/hazelcast-go-client/internal/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/internal/security"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

var nextId int32

func StartNewClient() (*Client, error) {
	if client, err := NewClient(); err != nil {
		return nil, err
	} else if err = client.Start(); err != nil {
		return nil, err
	} else {
		return client, nil
	}
}

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
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewClient() (*Client, error) {
	return NewClientWithConfig(NewConfigBuilder())
}

// NewClientWithConfig creates and returns a new client with the given config.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
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
	proxyManager        *proxy.Manager
	connectionManager   *icluster.ConnectionManager
	clusterService      *icluster.ServiceImpl
	partitionService    *icluster.PartitionService
	eventDispatcher     *event.DispatchService
	userEventDispatcher *event.DispatchService
	invocationHandler   invocation.Handler
	logger              logger.Logger

	// state
	started atomic.Value
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
	client.started.Store(false)
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
func (c *Client) GetMap(name string) (hztypes.Map, error) {
	c.ensureStarted()
	m, err := c.proxyManager.GetMap(name)
	return m.(hztypes.Map), err
}

// GetReplicatedMap returns a replicated map instance.
func (c *Client) GetReplicatedMap(name string) (hztypes.ReplicatedMap, error) {
	c.ensureStarted()
	m, err := c.proxyManager.GetReplicatedMap(name)
	return m.(hztypes.ReplicatedMap), err
}

// Start connects the client to the cluster.
func (c *Client) Start() error {
	// TODO: Recover from panics and return as error
	if c.started.Load() == true {
		return nil
	}
	c.eventDispatcher.Publish(ilifecycle.NewStateChanged(lifecycle.StateStarting))
	clusterServiceStartCh := c.clusterService.Start(c.clusterConfig.SmartRouting)
	c.partitionService.Start()
	if err := c.connectionManager.Start(); err != nil {
		return err
	}
	<-clusterServiceStartCh
	c.started.Store(true)
	c.eventDispatcher.Publish(ilifecycle.NewStateChanged(lifecycle.StateStarted))
	return nil
}

// Shutdown disconnects the client from the cluster.
func (c Client) Shutdown() {
	if c.started.Load() != true {
		return
	}
	c.eventDispatcher.Publish(ilifecycle.NewStateChanged(lifecycle.StateShuttingDown))
	c.clusterService.Stop()
	c.partitionService.Stop()
	<-c.connectionManager.Stop()
	c.eventDispatcher.Publish(ilifecycle.NewStateChanged(lifecycle.StateShutDown))
}

// ListenLifecycleStateChange adds a lifecycle state change handler with a unique subscription ID.
// The handler must not block.
func (c *Client) ListenLifecycleStateChange(subscriptionID int, handler lifecycle.StateChangeHandler) {
	c.userEventDispatcher.SubscribeSync(internal.LifecycleEventStateChanged, subscriptionID, func(event event.Event) {
		if stateChangeEvent, ok := event.(*lifecycle.StateChanged); ok {
			handler(*stateChangeEvent)
		} else {
			c.logger.Errorf("cannot cast event to lifecycle.StateChanged event")
		}
	})
}

// UnlistenLifecycleStateChange removes the lifecycle state change handler with the given subscription ID
func (c *Client) UnlistenLifecycleStateChange(subscriptionID int) {
	c.userEventDispatcher.Unsubscribe(internal.LifecycleEventStateChanged, subscriptionID)
}

// ListenLifecycleStateChange adds a member state change handler with a unique subscription ID.
func (c *Client) ListenMemberStateChange(subscriptionID int, handler cluster.MemberStateChangedHandler) {
	c.userEventDispatcher.Subscribe(icluster.EventMembersAdded, subscriptionID, func(event event.Event) {
		if membersAddedEvent, ok := event.(*icluster.MembersAdded); ok {
			for _, member := range membersAddedEvent.Members {
				handler(cluster.MemberStateChanged{
					State:  cluster.MemberStateAdded,
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
				handler(cluster.MemberStateChanged{
					State:  cluster.MemberStateRemoved,
					Member: member,
				})
			}
		} else {
			c.logger.Errorf("cannot cast event to cluster.MembersRemoved event")
		}
	})
}

// UnlistenMemberStateChange removes the member state change handler with the given subscription ID.
func (c *Client) UnlistenMemberStateChange(subscriptionID int) {
	c.userEventDispatcher.Unsubscribe(icluster.EventMembersAdded, subscriptionID)
	c.userEventDispatcher.Unsubscribe(icluster.EventMembersRemoved, subscriptionID)
}

func (c *Client) ensureStarted() {
	if c.started.Load() == false {
		panic("client not started")
	}
}

func (c *Client) subscribeUserEvents() {
	c.eventDispatcher.SubscribeSync(internal.LifecycleEventStateChanged, event.DefaultSubscriptionID, func(event event.Event) {
		c.userEventDispatcher.Publish(event)
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
	addressTranslator := cluster.NewDefaultAddressTranslator()
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
		AddressTranslator:    addressTranslator,
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
	c.proxyManager = proxy.NewManager(proxyManagerServiceBundle)
	c.invocationHandler = invocationHandler
}
