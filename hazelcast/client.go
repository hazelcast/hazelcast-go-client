package hazelcast

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/property"
	icluster "github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	ilifecycle "github.com/hazelcast/hazelcast-go-client/v4/internal/lifecycle"
	iproperty "github.com/hazelcast/hazelcast-go-client/v4/internal/property"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/security"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

var nextId int32

type Client struct {
	// configuration
	name          string
	clusterConfig *cluster.Config

	// components
	proxyManager      *proxy.Manager
	connectionManager *icluster.ConnectionManager
	clusterService    *icluster.ServiceImpl
	partitionService  *icluster.PartitionService
	eventDispatcher   *event.DispatchService
	invocationHandler invocation.Handler
	logger            logger.Logger

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
	iproperty.UpdateWithMissingProps(config.Properties)
	logLevel, err := logger.GetLogLevel(config.Properties[property.LoggingLevel])
	if err != nil {
		return nil, err
	}
	clientLogger := logger.NewWithLevel(logLevel)
	impl := &Client{
		name:          name,
		clusterConfig: &config.ClusterConfig,
		logger:        clientLogger,
	}
	impl.started.Store(false)
	impl.createComponents(&config)
	return impl, nil
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
	c.eventDispatcher.Publish(ilifecycle.NewStateChangedImpl(lifecycle.StateStarting))
	clusterServiceStartCh := c.clusterService.Start(c.clusterConfig.SmartRouting)
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
func (c Client) Shutdown() {
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
func (c *Client) ListenLifecycleStateChange(handler lifecycle.StateChangeHandler) {
	// derive subscriptionID from the handler
	subscriptionID := event.MakeSubscriptionID(handler)
	c.eventDispatcher.SubscribeSync(lifecycle.EventStateChanged, subscriptionID, func(event event.Event) {
		if stateChangeEvent, ok := event.(*lifecycle.StateChanged); ok {
			handler(*stateChangeEvent)
		} else {
			panic("cannot cast event to lifecycle.StateChanged event")
		}
	})
}

//func (c *Client) ExecuteSQL(sql string) (sql.Result, error) {
//	panic("implement me: ExecuteSQL")
//}

func (c *Client) ensureStarted() {
	if c.started.Load() == false {
		panic("client not started")
	}
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
	eventDispatcher := event.NewDispatchService()
	requestCh := make(chan invocation.Invocation, 1024)
	partitionService := icluster.NewPartitionService(icluster.PartitionServiceCreationBundle{
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
	proxyManager := proxy.NewManager(proxyManagerServiceBundle)

	c.eventDispatcher = eventDispatcher
	c.connectionManager = connectionManager
	c.clusterService = clusterService
	c.partitionService = partitionService
	c.proxyManager = proxyManager
	c.invocationHandler = invocationHandler
}
