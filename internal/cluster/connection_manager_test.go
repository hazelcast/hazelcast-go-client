package cluster

import (
	"context"
	"errors"
	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/security"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	publogger "github.com/hazelcast/hazelcast-go-client/logger"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type CheckedAddressHelper struct {
	error   error
	address pubcluster.Address
}

type MockMemberConnectivityChecker struct {
	mock.Mock
}

func (c *MockMemberConnectivityChecker) checkConnectivity(ctx context.Context, addr pubcluster.Address) (pubcluster.Address, error) {
	args := c.Called(ctx, addr)
	return args.Get(0).(pubcluster.Address), args.Error(1)
}

func TestConnectCluster_NoAddressAvailable(t *testing.T) {
	// inputs
	inputAddresses := []pubcluster.Address{"127.0.0.1:0"}
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5703", error: errors.New("cannot connect to address in the cluster")},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}

	// perform
	checker, _, err := clusterConnectTest(t, checkedAddresses, inputAddresses, portRange)

	// assert
	assert.NotNil(t, checker)
	checker.AssertNumberOfCalls(t, "checkConnectivity", 3)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "cannot connect to any address in the cluster")
}

func TestConnectCluster_LastAddressAvailable(t *testing.T) {
	// inputs
	inputAddresses := []pubcluster.Address{"127.0.0.1:0"}
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5703", error: nil},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}

	// perform
	checker, _, err := clusterConnectTest(t, checkedAddresses, inputAddresses, portRange)

	// assert
	assert.NotNil(t, checker)
	checker.AssertNumberOfCalls(t, "checkConnectivity", 3)
	assert.Nil(t, err)
}

func TestConnectCluster_SecondAddressAvailable(t *testing.T) {
	// inputs
	inputAddresses := []pubcluster.Address{"127.0.0.1:0"}
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", error: nil},
		{address: "127.0.0.1:5703", error: nil},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}

	// perform
	checker, _, err := clusterConnectTest(t, checkedAddresses, inputAddresses, portRange)

	// assert
	assert.NotNil(t, checker)
	checker.AssertNumberOfCalls(t, "checkConnectivity", 2)
	assert.Nil(t, err)
}

func TestConnectCluster_MultipleAddressesAvailable(t *testing.T) {
	// inputs
	inputAddresses := []pubcluster.Address{"127.0.0.1:0", "192.168.1.2:6000"}
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", error: nil},
		{address: "127.0.0.1:5703", error: nil},
		{address: "192.168.1.2:6000", error: nil},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}

	// perform
	checker, _, err := clusterConnectTest(t, checkedAddresses, inputAddresses, portRange)

	// assert
	assert.NotNil(t, checker)
	checker.AssertNumberOfCalls(t, "checkConnectivity", 3)
	assert.Nil(t, err)
}

// clusterConnectTest makes it easier to perform cluster connectivity tests
func clusterConnectTest(t *testing.T, checkedAddresses []CheckedAddressHelper, inputAddresses []pubcluster.Address, portRange pubcluster.PortRange) (*MockMemberConnectivityChecker, pubcluster.Address, error) {
	// input data
	ctx := context.TODO()
	checker := &MockMemberConnectivityChecker{}
	for _, addr := range checkedAddresses {
		checker.On("checkConnectivity",
			ctx,
			addr.address,
		).Return(addr.address, addr.error)
	}

	// perform
	cm, err := getConnectionManager(checker, pubcluster.Config{
		Network: pubcluster.NetworkConfig{
			PortRange: portRange,
		},
	}, inputAddresses)
	if err != nil {
		t.Fatal(err)
	}
	resultAddr, err := cm.connectCluster(ctx, true)
	return checker, resultAddr, err
}

// getConnectionManager creates a connection manager with minimum configuration and mocked connectivity checker
func getConnectionManager(checker *MockMemberConnectivityChecker, config pubcluster.Config, addresses []pubcluster.Address) (*ConnectionManager, error) {
	logLevel, err := ilogger.GetLogLevel(publogger.InfoLevel)
	if err != nil {
		return nil, err
	}
	logger := ilogger.NewWithLevel(logLevel)
	credentials := security.NewUsernamePasswordCredentials("", "")
	urgentRequestCh := make(chan invocation.Invocation, 1024)
	responseCh := make(chan *proto.ClientMessage, 1024)
	partitionService := NewPartitionService(PartitionServiceCreationBundle{
		EventDispatcher: event.NewDispatchService(logger),
		Logger:          logger,
	})
	invocationFactory := NewConnectionInvocationFactory(&config)
	clusterService := NewService(CreationBundle{
		AddrProvider:      DefaultAddressProvider{addresses: addresses},
		RequestCh:         urgentRequestCh,
		InvocationFactory: invocationFactory,
		EventDispatcher:   event.NewDispatchService(logger),
		PartitionService:  partitionService,
		Logger:            logger,
		Config:            &config,
		AddressTranslator: NewDefaultAddressTranslator(),
	})
	serService, _ := serialization.NewService(&pubserialization.Config{})
	manager := NewConnectionManager(ConnectionManagerCreationBundle{
		RequestCh:            urgentRequestCh,
		ResponseCh:           responseCh,
		Logger:               logger,
		ClusterService:       clusterService,
		PartitionService:     partitionService,
		SerializationService: serService,
		EventDispatcher:      event.NewDispatchService(logger),
		InvocationFactory:    invocationFactory,
		ClusterConfig:        &config,
		Credentials:          credentials,
		ClientName:           "test",
		AddrTranslator:       NewDefaultAddressTranslator(),
		Labels:               []string{},
	})
	manager.memberConnectivityChecker = checker
	return manager, nil
}
