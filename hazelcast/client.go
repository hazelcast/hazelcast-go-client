package hazelcast

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	icluster "github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	ilifecycle "github.com/hazelcast/hazelcast-go-client/v4/internal/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/security"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

var nextId int32

type Client interface {
	// attributes
	Name() string

	// control
	Start() error
	Stop()

	// events
	ListenLifecycleStateChange(handler lifecycle.StateChangeHandler)

	// access to data structures
	GetMap(name string) (hztypes.Map, error)
}

type clientImpl struct {
	// configuration
	name          string
	clusterName   string
	networkConfig *icluster.NetworkConfig

	// components
	proxyManager      proxy.Manager
	connectionManager icluster.ConnectionManager
	eventDispatcher   event.DispatchService
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

func (c *clientImpl) GetMap(name string) (hztypes.Map, error) {
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
	c.eventDispatcher.Publish(ilifecycle.NewStateChangedImpl(lifecycle.StateClientConnected))
	return nil
}

func (c clientImpl) Stop() {
	// TODO: shutdown
	c.connectionManager.Stop()
	c.eventDispatcher.Publish(ilifecycle.NewStateChangedImpl(lifecycle.StateClientDisconnected))
}

func (c *clientImpl) ListenLifecycleStateChange(handler lifecycle.StateChangeHandler) {
	// derive subscriptionID from the handler
	subscriptionID := int(reflect.ValueOf(handler).Pointer())
	c.eventDispatcher.Subscribe(ilifecycle.EventStateChanged, subscriptionID, func(event event.Event) {
		// cast event to StateChanged
		if stateChangeEvent, ok := event.(lifecycle.StateChanged); ok {
			handler(stateChangeEvent)
		} else {
			panic("cannot cast event to lifecycle.StateChanged event")
		}
	})
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
	addressProviders := []icluster.AddressProvider{
		icluster.NewDefaultAddressProvider(config.Network),
	}
	eventDispatcher := event.NewDispatchServiceImpl()
	clusterService := icluster.NewServiceImpl(addressProviders)
	partitionService := icluster.NewPartitionServiceImpl(icluster.PartitionServiceCreationBundle{
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
	connectionManager := icluster.NewConnectionManagerImpl(icluster.ConnectionManagerCreationBundle{
		RequestCh:            requestCh,
		ResponseCh:           responseCh,
		SmartRouting:         smartRouting,
		Logger:               c.logger,
		AddressTranslator:    addressTranslator,
		ClusterService:       clusterService,
		PartitionService:     partitionService,
		SerializationService: serializationService,
		EventDispatcher:      eventDispatcher,
		NetworkConfig:        config.Network,
		Credentials:          credentials,
		ClientName:           c.name,
	})
	invocationHandler := icluster.NewConnectionInvocationHandler(icluster.ConnectionInvocationHandlerCreationBundle{
		ConnectionManager: connectionManager,
		ClusterService:    clusterService,
		SmartRouting:      smartRouting,
		Logger:            c.logger,
	})
	invocationService.SetHandler(invocationHandler)
	invocationFactory := icluster.NewConnectionInvocationFactory(partitionService, 120*time.Second)
	proxyManagerServiceBundle := proxy.CreationBundle{
		RequestCh:            requestCh,
		SerializationService: serializationService,
		PartitionService:     partitionService,
		ClusterService:       clusterService,
		SmartRouting:         smartRouting,
		InvocationFactory:    invocationFactory,
	}
	proxyManager := proxy.NewManagerImpl(proxyManagerServiceBundle)

	c.eventDispatcher = eventDispatcher
	c.connectionManager = connectionManager
	c.proxyManager = proxyManager
}
