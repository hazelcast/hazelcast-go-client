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
	"sync"
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/client"
	icluster "github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	isql "github.com/hazelcast/hazelcast-go-client/internal/sql"
	"github.com/hazelcast/hazelcast-go-client/sql"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	ClientVersion = internal.ClientVersion
)

// StartNewClient creates and starts a new client with the default configuration.
// The default configuration is tuned connect to an Hazelcast cluster running on the same computer with the client.
func StartNewClient(ctx context.Context) (*Client, error) {
	return StartNewClientWithConfig(ctx, NewConfig())
}

// StartNewClientWithConfig creates and starts a new client with the given configuration.
func StartNewClientWithConfig(ctx context.Context, config Config) (*Client, error) {
	c, err := newClient(config)
	if err != nil {
		return nil, err
	}
	if err = c.ic.Start(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Client enables you to do all Hazelcast operations without being a member of the cluster.
// It connects to one or more of the cluster members and delegates all cluster wide operations to them.
type Client struct {
	membershipListenerMapMu *sync.Mutex
	proxyManager            *proxyManager
	membershipListenerMap   map[types.UUID]int64
	lifecycleListenerMap    map[types.UUID]int64
	lifecycleListenerMapMu  *sync.Mutex
	ic                      *client.Client
	sqlService              isql.Service
}

func newClient(config Config) (*Client, error) {
	config = config.Clone()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	icc := &client.Config{
		Name:          config.ClientName,
		Cluster:       &config.Cluster,
		Failover:      &config.Failover,
		Serialization: &config.Serialization,
		Logger:        &config.Logger,
		Labels:        config.Labels,
		StatsEnabled:  config.Stats.Enabled,
		StatsPeriod:   time.Duration(config.Stats.Period),
	}
	ic, err := client.New(icc)
	if err != nil {
		return nil, err
	}
	c := &Client{
		ic:                      ic,
		lifecycleListenerMap:    map[types.UUID]int64{},
		lifecycleListenerMapMu:  &sync.Mutex{},
		membershipListenerMap:   map[types.UUID]int64{},
		membershipListenerMapMu: &sync.Mutex{},
	}
	c.addConfigEvents(&config)
	c.createComponents(&config)
	return c, nil
}

// Name returns client's name
// Use config.Name to set the client name.
// If not set manually, an automatically generated name is used.
func (c *Client) Name() string {
	return c.ic.Name()
}

// GetList returns a list instance.
func (c *Client) GetList(ctx context.Context, name string) (*List, error) {
	if c.ic.State() != client.Ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getList(ctx, name)
}

// GetMap returns a distributed map instance.
func (c *Client) GetMap(ctx context.Context, name string) (*Map, error) {
	if c.ic.State() != client.Ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getMap(ctx, name)
}

// GetReplicatedMap returns a replicated map instance.
func (c *Client) GetReplicatedMap(ctx context.Context, name string) (*ReplicatedMap, error) {
	if c.ic.State() != client.Ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getReplicatedMap(ctx, name)
}

// GetMultiMap returns a MultiMap instance.
func (c *Client) GetMultiMap(ctx context.Context, name string) (*MultiMap, error) {
	if c.ic.State() != client.Ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getMultiMap(ctx, name)
}

// GetQueue returns a queue instance.
func (c *Client) GetQueue(ctx context.Context, name string) (*Queue, error) {
	if c.ic.State() != client.Ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getQueue(ctx, name)
}

// GetTopic returns a topic instance.
func (c *Client) GetTopic(ctx context.Context, name string) (*Topic, error) {
	if c.ic.State() != client.Ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getTopic(ctx, name)
}

// GetSet returns a set instance.
func (c *Client) GetSet(ctx context.Context, name string) (*Set, error) {
	if c.ic.State() != client.Ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getSet(ctx, name)
}

// GetPNCounter returns a PNCounter instance.
func (c *Client) GetPNCounter(ctx context.Context, name string) (*PNCounter, error) {
	if c.ic.State() != client.Ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getPNCounter(ctx, name)
}

// GetFlakeIDGenerator returns a FlakeIDGenerator instance.
func (c *Client) GetFlakeIDGenerator(ctx context.Context, name string) (*FlakeIDGenerator, error) {
	if c.ic.State() != client.Ready {
		return nil, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.getFlakeIDGenerator(ctx, name)
}

// GetDistributedObjectsInfo returns the information of all objects created cluster-wide.
func (c *Client) GetDistributedObjectsInfo(ctx context.Context) ([]types.DistributedObjectInfo, error) {
	if c.ic.State() != client.Ready {
		return nil, hzerrors.ErrClientNotActive
	}
	request := codec.EncodeClientGetDistributedObjectsRequest()
	resp, err := c.proxyManager.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	return codec.DecodeClientGetDistributedObjectsResponse(resp), nil
}

// Shutdown disconnects the client from the cluster and frees resources allocated by the client.
func (c *Client) Shutdown(ctx context.Context) error {
	return c.ic.Shutdown(ctx)
}

// Running returns true if the client is running.
func (c *Client) Running() bool {
	return c.ic.State() == client.Ready
}

// AddLifecycleListener adds a lifecycle state change handler after the client starts.
// Use the returned subscription ID to remove the listener.
// The handler must not block.
func (c *Client) AddLifecycleListener(handler LifecycleStateChangeHandler) (types.UUID, error) {
	if c.ic.State() >= client.Stopping {
		return types.UUID{}, hzerrors.ErrClientNotActive
	}
	uuid := types.NewUUID()
	subscriptionID := event.NextSubscriptionID()
	c.addLifecycleListener(subscriptionID, handler)
	c.lifecycleListenerMapMu.Lock()
	c.lifecycleListenerMap[uuid] = subscriptionID
	c.lifecycleListenerMapMu.Unlock()
	return uuid, nil
}

// RemoveLifecycleListener removes the lifecycle state change handler with the given subscription ID
func (c *Client) RemoveLifecycleListener(subscriptionID types.UUID) error {
	if c.ic.State() >= client.Stopping {
		return hzerrors.ErrClientNotActive
	}
	c.lifecycleListenerMapMu.Lock()
	if intID, ok := c.lifecycleListenerMap[subscriptionID]; ok {
		c.ic.EventDispatcher.Unsubscribe(eventLifecycleEventStateChanged, intID)
		delete(c.lifecycleListenerMap, subscriptionID)
	}
	c.lifecycleListenerMapMu.Unlock()
	return nil
}

// AddMembershipListener adds a member state change handler and returns a unique subscription ID.
// Use the returned subscription ID to remove the listener.
func (c *Client) AddMembershipListener(handler cluster.MembershipStateChangeHandler) (types.UUID, error) {
	if c.ic.State() >= client.Stopping {
		return types.UUID{}, hzerrors.ErrClientNotActive
	}
	uuid := types.NewUUID()
	subscriptionID := event.NextSubscriptionID()
	c.addMembershipListener(subscriptionID, handler)
	c.membershipListenerMapMu.Lock()
	c.membershipListenerMap[uuid] = subscriptionID
	c.membershipListenerMapMu.Unlock()
	return uuid, nil
}

// RemoveMembershipListener removes the member state change handler with the given subscription ID.
func (c *Client) RemoveMembershipListener(subscriptionID types.UUID) error {
	if c.ic.State() >= client.Stopping {
		return hzerrors.ErrClientNotActive
	}
	c.membershipListenerMapMu.Lock()
	if intID, ok := c.membershipListenerMap[subscriptionID]; ok {
		c.ic.EventDispatcher.Unsubscribe(icluster.EventMembers, intID)
		delete(c.membershipListenerMap, subscriptionID)
	}
	c.membershipListenerMapMu.Unlock()
	return nil
}

// AddDistributedObjectListener adds a distributed object listener and returns a unique subscription ID.
// Use the returned subscription ID to remove the listener.
func (c *Client) AddDistributedObjectListener(ctx context.Context, handler DistributedObjectNotifiedHandler) (types.UUID, error) {
	if c.ic.State() >= client.Stopping {
		return types.UUID{}, hzerrors.ErrClientNotActive
	}
	return c.proxyManager.addDistributedObjectEventListener(ctx, handler)
}

// RemoveDistributedObjectListener removes the distributed object listener handler with the given subscription ID.
func (c *Client) RemoveDistributedObjectListener(ctx context.Context, subscriptionID types.UUID) error {
	if c.ic.State() >= client.Stopping {
		return hzerrors.ErrClientNotActive
	}
	return c.proxyManager.removeDistributedObjectEventListener(ctx, subscriptionID)
}

// SQL returns a service to execute distributes SQL queries.
func (c *Client) SQL() sql.Service {
	return c.sqlService
}

func (c *Client) addLifecycleListener(subscriptionID int64, handler LifecycleStateChangeHandler) {
	c.ic.EventDispatcher.Subscribe(eventLifecycleEventStateChanged, subscriptionID, func(event event.Event) {
		// This is a workaround to avoid cyclic dependency between internal/cluster and hazelcast package.
		// A better solution would have been separating lifecycle events to its own package where both internal/cluster and hazelcast packages can access(but this is an API breaking change).
		// The workaround is that we have two lifecycle events one is internal(inside internal/cluster) other is public.
		// This is because internal/cluster can not use hazelcast package.
		// We map internal ones to external ones on the handler before giving them to the user here.
		e := event.(*lifecycle.StateChangedEvent)
		var mapped LifecycleState
		switch e.State {
		case lifecycle.StateStarting:
			mapped = LifecycleStateStarting
		case lifecycle.StateStarted:
			mapped = LifecycleStateStarted
		case lifecycle.StateShuttingDown:
			mapped = LifecycleStateShuttingDown
		case lifecycle.StateShutDown:
			mapped = LifecycleStateShutDown
		case lifecycle.StateConnected:
			mapped = LifecycleStateConnected
		case lifecycle.StateDisconnected:
			mapped = LifecycleStateDisconnected
		case lifecycle.StateChangedCluster:
			mapped = LifecycleStateChangedCluster
		default:
			c.ic.Logger.Warnf("no corresponding hazelcast.LifecycleStateChanged event found : %v", e.State)
			return
		}
		handler(*newLifecycleStateChanged(mapped))
	})
}

func (c *Client) addMembershipListener(subscriptionID int64, handler cluster.MembershipStateChangeHandler) {
	c.ic.EventDispatcher.Subscribe(icluster.EventMembers, subscriptionID, func(event event.Event) {
		e := event.(*icluster.MembersStateChangedEvent)
		if e.State == icluster.MembersStateAdded {
			for _, member := range e.Members {
				handler(cluster.MembershipStateChanged{
					State:  cluster.MembershipStateAdded,
					Member: member,
				})
			}
			return
		}
		for _, member := range e.Members {
			handler(cluster.MembershipStateChanged{
				State:  cluster.MembershipStateRemoved,
				Member: member,
			})
		}

	})
}

func (c *Client) addConfigEvents(config *Config) {
	for uuid, handler := range config.lifecycleListeners {
		subscriptionID := event.NextSubscriptionID()
		c.addLifecycleListener(subscriptionID, handler)
		c.lifecycleListenerMap[uuid] = subscriptionID
	}
	for uuid, handler := range config.membershipListeners {
		subscriptionID := event.NextSubscriptionID()
		c.addMembershipListener(subscriptionID, handler)
		c.membershipListenerMap[uuid] = subscriptionID
	}
}

func (c *Client) createComponents(config *Config) {
	listenerBinder := icluster.NewConnectionListenerBinder(
		c.ic.ConnectionManager,
		c.ic.InvocationService,
		c.ic.InvocationFactory,
		c.ic.EventDispatcher,
		c.ic.Logger,
		!config.Cluster.Unisocket)
	proxyManagerServiceBundle := creationBundle{
		InvocationService:    c.ic.InvocationService,
		SerializationService: c.ic.SerializationService,
		PartitionService:     c.ic.PartitionService,
		ClusterService:       c.ic.ClusterService,
		Config:               config,
		InvocationFactory:    c.ic.InvocationFactory,
		ListenerBinder:       listenerBinder,
		Logger:               c.ic.Logger,
	}
	c.proxyManager = newProxyManager(proxyManagerServiceBundle)
	c.sqlService = isql.NewService(c.ic.ConnectionManager, c.ic.SerializationService, c.ic.InvocationFactory, c.ic.InvocationService, &c.ic.Logger)
}
