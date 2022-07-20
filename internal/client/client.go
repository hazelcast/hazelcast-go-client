/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package client

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/check"
	"github.com/hazelcast/hazelcast-go-client/internal/cloud"
	icluster "github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/lifecycle"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/stats"
	"github.com/hazelcast/hazelcast-go-client/logger"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

var nextId int32

const (
	Created int32 = iota
	Starting
	Ready
	Stopping
	Stopped
)

var handleClusterEventSubID = event.NextSubscriptionID()

type Config struct {
	Name          string
	Cluster       *cluster.Config
	Failover      *cluster.FailoverConfig
	Serialization *pubserialization.Config
	Logger        *logger.Config
	Labels        []string
	StatsEnabled  bool
	StatsPeriod   time.Duration
}

func NewConfig() *Config {
	return &Config{
		Cluster:       &cluster.Config{},
		Failover:      &cluster.FailoverConfig{},
		Serialization: &pubserialization.Config{},
		Logger:        &logger.Config{},
	}
}

func (c *Config) Validate() error {
	if err := c.Cluster.Validate(); err != nil {
		return err
	}
	if err := c.Failover.Validate(*c.Cluster); err != nil {
		return err
	}
	if err := c.Serialization.Validate(); err != nil {
		return err
	}
	if err := c.Logger.Validate(); err != nil {
		return err
	}
	if err := check.EnsureNonNegativeDuration(&c.StatsPeriod, 5*time.Second, "invalid period"); err != nil {
		return err
	}
	return nil
}

type ShutdownHandler func(ctx context.Context)

type ShutdownHandlerType int

const ProxyShutdownHandler ShutdownHandlerType = iota

func executeShutdownHandlers(
	ctx context.Context,
	m map[ShutdownHandlerType]func(ctx context.Context)) {
	for _, f := range m {
		f(ctx)
	}
}

type Client struct {
	Logger               ilogger.LogAdaptor
	ConnectionManager    *icluster.ConnectionManager
	ViewListenerService  *icluster.ViewListenerService
	InvocationService    *invocation.Service
	InvocationFactory    *icluster.ConnectionInvocationFactory
	SerializationService *serialization.Service
	EventDispatcher      *event.DispatchService
	StatsService         *stats.Service
	heartbeatService     *icluster.HeartbeatService
	clusterConfig        *cluster.Config
	PartitionService     *icluster.PartitionService
	ClusterService       *icluster.Service
	name                 string
	shutdownHandlers     []ShutdownHandler
	state                int32
}

func New(config *Config) (*Client, error) {
	id := atomic.AddInt32(&nextId, 1)
	name := config.Name
	if name == "" {
		name = fmt.Sprintf("hz.client_%d", id)
	}
	clientLogger, err := loggerFromConf(config.Logger)
	if err != nil {
		return nil, err
	}
	serService, err := serialization.NewService(config.Serialization)
	if err != nil {
		return nil, err
	}
	clientLogger.Trace(func() string { return fmt.Sprintf("creating new client: %s", name) })
	c := &Client{
		name:                 name,
		clusterConfig:        config.Cluster,
		SerializationService: serService,
		EventDispatcher:      event.NewDispatchService(clientLogger),
		Logger:               clientLogger,
	}
	c.createComponents(config)
	return c, nil
}

func (c *Client) AddShutdownHandler(f ShutdownHandler) {
	// this is supposed to be called during client initialization, so there's no risk of races.
	c.shutdownHandlers = append(c.shutdownHandlers, f)
}

// Name returns client's name.
// Use config.Name to set the client name.
// If not set manually, an automatically generated name is used.
func (c *Client) Name() string {
	return c.name
}

func (c *Client) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&c.state, Created, Starting) {
		return nil
	}
	c.EventDispatcher.Publish(lifecycle.NewLifecycleStateChanged(lifecycle.StateStarting))
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.ConnectionManager.Start(ctx); err != nil {
		// ignoring the event dispatcher stop
		_ = c.EventDispatcher.Stop(ctx)
		c.InvocationService.Stop()
		return err
	}
	c.heartbeatService.Start()
	if c.StatsService != nil {
		c.StatsService.Start()
	}
	c.EventDispatcher.Subscribe(icluster.EventCluster, handleClusterEventSubID, c.handleClusterEvent)
	atomic.StoreInt32(&c.state, Ready)
	c.EventDispatcher.Publish(lifecycle.NewLifecycleStateChanged(lifecycle.StateStarted))
	return nil
}

// Shutdown disconnects the client from the cluster and frees resources allocated by the client.
func (c *Client) Shutdown(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&c.state, Ready, Stopping) {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.EventDispatcher.Publish(lifecycle.NewLifecycleStateChanged(lifecycle.StateShuttingDown))
	executeShutdownHandlers(ctx, c.ShutdownHandlers)
	c.InvocationService.Stop()
	c.heartbeatService.Stop()
	c.ConnectionManager.Stop()
	if c.StatsService != nil {
		c.StatsService.Stop()
	}
	for _, h := range c.shutdownHandlers {
		h(ctx)
	}
	atomic.StoreInt32(&c.state, Stopped)
	c.EventDispatcher.Publish(lifecycle.NewLifecycleStateChanged(lifecycle.StateShutDown))
	if err := c.EventDispatcher.Stop(ctx); err != nil {
		return err
	}
	return nil
}

func (c *Client) State() int32 {
	return atomic.LoadInt32(&c.state)
}

func (c *Client) createComponents(config *Config) {
	partitionService := icluster.NewPartitionService(icluster.PartitionServiceCreationBundle{
		EventDispatcher: c.EventDispatcher,
		Logger:          c.Logger,
	})
	c.InvocationFactory = icluster.NewConnectionInvocationFactory(config.Cluster)
	var failoverConfigs []cluster.Config
	maxTryCount := math.MaxInt32
	if config.Failover.Enabled {
		maxTryCount = config.Failover.TryCount
		failoverConfigs = config.Failover.Configs
	}
	failoverService := icluster.NewFailoverService(c.Logger,
		maxTryCount, *config.Cluster, failoverConfigs, addrProviderTranslator)
	clusterService := icluster.NewService(icluster.CreationBundle{
		InvocationFactory: c.InvocationFactory,
		EventDispatcher:   c.EventDispatcher,
		PartitionService:  partitionService,
		Logger:            c.Logger,
		Config:            config.Cluster,
		FailoverService:   failoverService,
	})
	connectionManager := icluster.NewConnectionManager(icluster.ConnectionManagerCreationBundle{
		Logger:               c.Logger,
		ClusterService:       clusterService,
		PartitionService:     partitionService,
		SerializationService: c.SerializationService,
		EventDispatcher:      c.EventDispatcher,
		InvocationFactory:    c.InvocationFactory,
		ClusterConfig:        config.Cluster,
		IsClientShutdown: func() bool {
			return atomic.LoadInt32(&c.state) == Stopped
		},
		ClientName:      c.name,
		FailoverService: failoverService,
		FailoverConfig:  config.Failover,
		Labels:          config.Labels,
	})
	viewListener := icluster.NewViewListenerService(clusterService, connectionManager, c.EventDispatcher, c.Logger)
	invocationHandler := icluster.NewConnectionInvocationHandler(icluster.ConnectionInvocationHandlerCreationBundle{
		ConnectionManager: connectionManager,
		ClusterService:    clusterService,
		Logger:            c.Logger,
		Config:            config.Cluster,
	})
	invocationService := invocation.NewService(invocationHandler, c.EventDispatcher, c.Logger)
	iv := time.Duration(c.clusterConfig.HeartbeatInterval)
	it := time.Duration(c.clusterConfig.HeartbeatTimeout)
	c.heartbeatService = icluster.NewHeartbeatService(connectionManager, c.InvocationFactory, invocationService, c.Logger, iv, it)
	if config.StatsEnabled {
		c.StatsService = stats.NewService(
			invocationService,
			c.InvocationFactory,
			c.EventDispatcher,
			c.Logger,
			config.StatsPeriod,
			c.name,
		)
	}
	c.ConnectionManager = connectionManager
	c.ClusterService = clusterService
	c.PartitionService = partitionService
	c.InvocationService = invocationService
	c.InvocationHandler = invocationHandler
	c.ViewListenerService = viewListener
	c.ConnectionManager.SetInvocationService(invocationService)
	c.ClusterService.SetInvocationService(invocationService)
}

func (c *Client) handleClusterEvent(event event.Event) {
	e := event.(*icluster.ClusterStateChangedEvent)
	if e.State == icluster.ClusterStateConnected {
		return
	}
	if atomic.LoadInt32(&c.state) != Ready {
		return
	}
	ctx := context.Background()
	if c.clusterConfig.ConnectionStrategy.ReconnectMode == cluster.ReconnectModeOff {
		c.Logger.Debug(func() string { return "reconnect mode is off, shutting down" })
		// Shutdown is blocking operation which will make sure all the event goroutines are closed.
		// If we wait here blocking, it will be a deadlock
		go c.Shutdown(ctx)
		return
	}
	c.Logger.Debug(func() string { return "cluster disconnected, rebooting" })
	// try to reboot cluster connection
	c.ConnectionManager.Stop()
	c.ClusterService.Reset()
	c.PartitionService.Reset()
	if err := c.ConnectionManager.Start(ctx); err != nil {
		c.Logger.Errorf("cannot reconnect to cluster, shutting down: %w", err)
		// Shutdown is blocking operation which will make sure all the event goroutines are closed.
		// If we wait here blocking, it will be a deadlock
		go c.Shutdown(ctx)
	}
}

func addrProviderTranslator(config *cluster.Config, logger ilogger.LogAdaptor) (icluster.AddressProvider, icluster.AddressTranslator) {
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

func loggerFromConf(config *logger.Config) (ilogger.LogAdaptor, error) {
	if config.CustomLogger != nil {
		return ilogger.LogAdaptor{Logger: config.CustomLogger}, nil
	}
	lg, err := ilogger.NewWithLevel(config.Level)
	if err != nil {
		return ilogger.LogAdaptor{}, err
	}
	return ilogger.LogAdaptor{Logger: lg}, nil
}
