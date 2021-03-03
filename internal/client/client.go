package client

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"sync/atomic"
	"time"
)

var nextId int32

type Client interface {
	Name() string
	GetMap(name string) (proxy.Map, error)
}

type Impl struct {
	// configuration
	name          string
	clusterName   string
	networkConfig *cluster.NetworkConfig

	// components
	proxyManager proxy.Manager
	//serializationService spi.SerializationService
	//partitionService     cluster.PartitionService
	//invocationService    invocation.Service
	//clusterService       cluster.Service
	connectionManager cluster.ConnectionManager
	logger            logger.Logger

	// state
	started bool
}

func NewImpl(name string, config Config) *Impl {
	id := atomic.AddInt32(&nextId, 1)
	if name == "" {
		name = fmt.Sprintf("hz.client_%d", id)
	}
	// TODO: consider disabling manual client name
	if config.ClientName != "" {
		name = config.ClientName
	}
	clientLogger := logger.New()
	// TODO: create services
	serializationService, err := serialization.NewService(serialization.NewConfig())
	if err != nil {
		panic(fmt.Errorf("error creating client: %w", err))
	}
	smartRouting := config.Network.SmartRouting
	addressTranslator := internal.NewDefaultAddressTranslator()
	addressProviders := []cluster.AddressProvider{
		cluster.NewDefaultAddressProvider(config.Network),
	}
	clusterService := cluster.NewServiceImpl(addressProviders)
	partitionService := cluster.NewPartitionServiceImpl(cluster.PartitionServiceCreationBundle{
		SerializationService: serializationService,
		Logger:               clientLogger,
	})
	connectionManager := cluster.NewConnectionManagerImpl(cluster.ConnectionManagerCreationBundle{
		SmartRouting:      smartRouting,
		Logger:            clientLogger,
		AddressTranslator: addressTranslator,
	})
	invocationHandler := cluster.NewConnectionInvocationHandler(cluster.ConnectionInvocationHandlerCreationBundle{
		ConnectionManager: connectionManager,
		ClusterService:    clusterService,
		SmartRouting:      smartRouting,
		Logger:            clientLogger,
	})
	invocationService := invocation.NewServiceImpl(invocation.ServiceCreationBundle{
		SmartRouting: smartRouting,
		Handler:      invocationHandler,
		Logger:       clientLogger,
	})
	invocationFactory := cluster.NewConnectionInvocationFactory(partitionService, 120*time.Second)
	proxyManagerServiceBundle := proxy.ProxyCreationBundle{
		SerializationService: serializationService,
		PartitionService:     partitionService,
		InvocationService:    invocationService,
		ClusterService:       clusterService,
		SmartRouting:         smartRouting,
		InvocationFactory:    invocationFactory,
	}
	return &Impl{
		name:              name,
		clusterName:       config.ClusterName,
		networkConfig:     &config.Network,
		proxyManager:      proxy.NewManagerImpl(proxyManagerServiceBundle),
		connectionManager: connectionManager,
		//invocationService: invocationService,
		logger: clientLogger,
	}
}

func (c *Impl) Name() string {
	return c.name
}

func (c *Impl) GetMap(name string) (proxy.Map, error) {
	return c.proxyManager.GetMap(name)
}

func (c *Impl) Start() error {
	// TODO: Recover from panics and return as error
	if c.started {
		return nil
	}
	c.connectionManager.Start()
	c.started = true
	return nil
}

/*
func (c ConnectionImpl) ClusterName() string {
	return c.clusterName
}

func (c ConnectionImpl) NetworkConfig() *NetworkConfig {
	return c.networkConfig
}
*/
