package client

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/security"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	pubproxy "github.com/hazelcast/hazelcast-go-client/v4/proxy"
	"sync/atomic"
	"time"
)

var nextId int32

type Client interface {
	Name() string
	GetMap(name string) (pubproxy.Map, error)
	Start() error
	Shutdown()
}

type clientImpl struct {
	// configuration
	name          string
	clusterName   string
	networkConfig *cluster.NetworkConfig

	// components
	proxyManager      proxy.Manager
	connectionManager cluster.ConnectionManager
	logger            logger.Logger

	// state
	started atomic.Value
}

func newClient(name string, config Config) *clientImpl {
	id := atomic.AddInt32(&nextId, 1)
	if name == "" {
		name = fmt.Sprintf("hz.client_%d", id)
	}
	// TODO: consider disabling manual client name
	if config.ClientName != "" {
		name = config.ClientName
	}
	clientLogger := logger.New()
	impl := &clientImpl{
		name:          name,
		clusterName:   config.ClusterName,
		networkConfig: &config.Network,
		logger:        clientLogger,
	}
	impl.started.Store(false)
	impl.createComponents(&config)
	return impl
}

func (c *clientImpl) Name() string {
	return c.name
}

func (c *clientImpl) GetMap(name string) (pubproxy.Map, error) {
	c.ensureStarted()
	return c.proxyManager.GetMap(name)
}

func (c *clientImpl) Start() error {
	// TODO: Recover from panics and return as error
	if c.started.Load() == true {
		return nil
	}
	if err := c.connectionManager.Start(); err != nil {
		return err
	}
	c.started.Store(true)
	return nil
}

func (c clientImpl) Shutdown() {

}

func (c *clientImpl) ensureStarted() {
	if c.started.Load() == false {
		panic("client not started")
	}
}

func (c *clientImpl) createComponents(config *Config) {
	credentials := security.NewUsernamePasswordCredentials("dev", "dev-pass")
	serializationService, err := serialization.NewService(serialization.NewConfig())
	if err != nil {
		panic(fmt.Errorf("error creating client: %w", err))
	}
	smartRouting := config.Network.SmartRouting()
	addressTranslator := internal.NewDefaultAddressTranslator()
	addressProviders := []cluster.AddressProvider{
		cluster.NewDefaultAddressProvider(config.Network),
	}
	clusterService := cluster.NewServiceImpl(addressProviders)
	partitionService := cluster.NewPartitionServiceImpl(cluster.PartitionServiceCreationBundle{
		SerializationService: serializationService,
		Logger:               c.logger,
	})
	requestCh := make(chan invocation.Invocation, 1)
	responseCh := make(chan *proto.ClientMessage, 1)
	invocationService := invocation.NewServiceImpl(invocation.ServiceCreationBundle{
		RequestCh:    requestCh,
		ResponseCh:   responseCh,
		SmartRouting: smartRouting,
		Logger:       c.logger,
	})
	connectionManager := cluster.NewConnectionManagerImpl(cluster.ConnectionManagerCreationBundle{
		RequestCh:            requestCh,
		ResponseCh:           responseCh,
		SmartRouting:         smartRouting,
		Logger:               c.logger,
		AddressTranslator:    addressTranslator,
		ClusterService:       clusterService,
		PartitionService:     partitionService,
		SerializationService: serializationService,
		NetworkConfig:        config.Network,
		Credentials:          credentials,
		ClientName:           c.name,
	})
	invocationHandler := cluster.NewConnectionInvocationHandler(cluster.ConnectionInvocationHandlerCreationBundle{
		ConnectionManager: connectionManager,
		ClusterService:    clusterService,
		SmartRouting:      smartRouting,
		Logger:            c.logger,
	})
	invocationService.SetHandler(invocationHandler)
	invocationFactory := cluster.NewConnectionInvocationFactory(partitionService, 120*time.Second)
	proxyManagerServiceBundle := proxy.CreationBundle{
		RequestCh:            requestCh,
		SerializationService: serializationService,
		PartitionService:     partitionService,
		ClusterService:       clusterService,
		SmartRouting:         smartRouting,
		InvocationFactory:    invocationFactory,
	}
	proxyManager := proxy.NewManagerImpl(proxyManagerServiceBundle)
	c.connectionManager = connectionManager
	c.proxyManager = proxyManager
}

/*
func (c ConnectionImpl) ClusterName() string {
	return c.clusterName
}

func (c ConnectionImpl) NetworkConfig() *NetworkConfig {
	return c.networkConfig
}
*/
