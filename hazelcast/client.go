package hazelcast

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	icluster "github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
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
	Shutdown()

	// events
	ListenLifecycleStateChange(handler lifecycle.StateChangeHandler)

	// access to data structures
	GetMap(name string) (hztypes.Map, error)
}

type clientImpl struct {
	// configuration
	name          string
	clusterName   string
	networkConfig cluster.NetworkConfig

	// components
	proxyManager      proxy.Manager
	connectionManager *icluster.ConnectionManager
	clusterService    *icluster.ServiceImpl
	partitionService  *icluster.PartitionServiceImpl
	eventDispatcher   event.DispatchService
	invocationHandler invocation.Handler
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
		networkConfig: config.Network,
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
	c.eventDispatcher.Publish(ilifecycle.NewStateChangedImpl(lifecycle.StateStarting))
	clusterServiceStartCh := c.clusterService.Start(c.networkConfig.SmartRouting())
	c.partitionService.Start()
	if err := c.connectionManager.Start(); err != nil {
		return err
	}
	<-clusterServiceStartCh
	c.started.Store(true)
	c.eventDispatcher.Publish(ilifecycle.NewStateChangedImpl(lifecycle.StateStarted))
	return nil
}

// Shutdown disconnects the client from the cluster.
// This call is blocking.
func (c clientImpl) Shutdown() {
	if c.started.Load() != true {
		return
	}
	c.eventDispatcher.Publish(ilifecycle.NewStateChangedImpl(lifecycle.StateShuttingDown))
	c.clusterService.Stop()
	c.partitionService.Stop()
	<-c.connectionManager.Stop()
	c.eventDispatcher.Publish(ilifecycle.NewStateChangedImpl(lifecycle.StateShutDown))
}

// ListenLifecycleStateChange adds a lifecycle state change handler.
// The handler must not block.
func (c *clientImpl) ListenLifecycleStateChange(handler lifecycle.StateChangeHandler) {
	// derive subscriptionID from the handler
	subscriptionID := event.MakeSubscriptionID(handler)
	c.eventDispatcher.SubscribeSync(ilifecycle.EventStateChanged, subscriptionID, func(event event.Event) {
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
	addressTranslator := cluster.NewDefaultAddressTranslator()
	addressProviders := []icluster.AddressProvider{
		icluster.NewDefaultAddressProvider(config.Network),
	}
	eventDispatcher := event.NewDispatchServiceImpl()
	requestCh := make(chan invocation.Invocation, 1024)
	partitionService := icluster.NewPartitionServiceImpl(icluster.PartitionServiceCreationBundle{
		SerializationService: serializationService,
		EventDispatcher:      eventDispatcher,
		Logger:               c.logger,
	})
	invocationFactory := icluster.NewConnectionInvocationFactory(partitionService, 120*time.Second)
	clusterService := icluster.NewServiceImpl(icluster.CreationBundle{
		AddrProviders:     addressProviders,
		RequestCh:         requestCh,
		InvocationFactory: invocationFactory,
		EventDispatcher:   eventDispatcher,
		Logger:            c.logger,
	})
	responseCh := make(chan *proto.ClientMessage, 1024)
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
		EventDispatcher:      eventDispatcher,
		NetworkConfig:        config.Network,
		Credentials:          credentials,
		ClientName:           c.name,
	})
	invocationHandler := icluster.NewConnectionInvocationHandler(icluster.ConnectionInvocationHandlerCreationBundle{
		ConnectionManager: connectionManager,
		ClusterService:    clusterService,
		Logger:            c.logger,
	})
	invocationService.SetHandler(invocationHandler)
	listenerBinder := icluster.NewConnectionListenerBinderImpl(connectionManager, requestCh)
	proxyManagerServiceBundle := proxy.CreationBundle{
		RequestCh:            requestCh,
		SerializationService: serializationService,
		PartitionService:     partitionService,
		ClusterService:       clusterService,
		SmartRouting:         smartRouting,
		InvocationFactory:    invocationFactory,
		EventDispatcher:      eventDispatcher,
		ListenerBinder:       listenerBinder,
	}
	proxyManager := proxy.NewManagerImpl(proxyManagerServiceBundle)

	c.eventDispatcher = eventDispatcher
	c.connectionManager = connectionManager
	c.clusterService = clusterService
	c.partitionService = partitionService
	c.proxyManager = proxyManager
	c.invocationHandler = invocationHandler
}
