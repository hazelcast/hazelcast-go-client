package client

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/connection"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/partition"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization/spi"
	"sync/atomic"
	"time"
)

var nextId int32

type Impl struct {
	// configuration
	name          string
	clusterName   string
	networkConfig *NetworkConfig
	logger        logger.Logger

	proxyManager         proxy.Manager
	serializationService spi.SerializationService
	partitionService     partition.Service
	invocationService    invocation.Service
	clusterService       cluster.Service
	connectionManager    connection.Manager
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
	clusterService := cluster.NewServiceImpl()
	partitionService := partition.NewServiceImpl(partition.CreationBundle{
		SerializationService: serializationService,
		Logger:               clientLogger,
	})
	connectionManager := connection.NewManagerImpl(connection.CreationBundle{
		SmartRouting:      smartRouting,
		Logger:            clientLogger,
		AddressTranslator: addressTranslator,
	})
	invocationHandler := connection.NewInvocationHandler(connection.InvocationHandlerCreationBundle{
		ConnectionManager: connectionManager,
		ClusterService:    clusterService,
		SmartRouting:      smartRouting,
		Logger:            clientLogger,
	})
	invocationService := invocation.NewServiceImpl(invocation.CreationBundle{
		SmartRouting:     smartRouting,
		PartitionService: partitionService,
		Handler:          invocationHandler,
		Logger:           clientLogger,
	})
	invocationFactory := connection.NewInvocationFactory(partitionService, 120*time.Second)
	proxyManagerServiceBundle := proxy.CreationBundle{
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
		invocationService: invocationService,
		logger:            clientLogger,
	}
}

func (c Impl) Name() string {
	return c.name
}

func (c Impl) GetMap(name string) (proxy.Map, error) {
	return c.proxyManager.GetMap(name)
}

/*
func (c Impl) ClusterName() string {
	return c.clusterName
}

func (c Impl) NetworkConfig() *NetworkConfig {
	return c.networkConfig
}
*/

func defaultConfig() Config {
	return Config{}
}
