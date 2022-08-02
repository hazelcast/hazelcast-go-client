package util

import (
	"context"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// WithoutK8s if WithoutK8s is set as an environmental variable, service runs locally.
// It assumes that there is a running Hazelcast cluster on port 5701 locally.
const WithoutK8s = "HZ_GO_SERVICE_WITHOUT_K8S"

const (
	ServiceName = "hazelcast-go-client-proxy-service"
	Port        = 8080
	// Timeout defines a common timeout value for all operation.
	Timeout = 5 * time.Second
)

var (
	ExampleMapName    = "myDistributedMap"
	ExampleMapEntries = []types.Entry{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
	}
)

// Service declares components of the service.
type Service struct {
	ServiceConfig *ServiceConfig
	Client        *hazelcast.Client
	ExampleMap    *hazelcast.Map
}

// NewDefaultService returns a new service which has default values for ServiceConfig and Hazelcast client.
func NewDefaultService(ctx context.Context) (*Service, error) {
	client, err := NewHazelcastClient(ctx)
	if err != nil {
		return nil, err
	}
	var exampleMap *hazelcast.Map
	if exampleMap, err = ExampleMap(ctx, client, ExampleMapName); err != nil {
		return nil, err
	}
	return &Service{
		ServiceConfig: NewDefaultServiceConfig(),
		Client:        client,
		ExampleMap:    exampleMap,
	}, nil
}

// NewDefaultServiceConfig returns a new ServiceConfig which has default values.
func NewDefaultServiceConfig() *ServiceConfig {
	return NewServiceConfig(ServiceName, Port, Timeout)
}

// ServiceConfig keeps information about service configuration.
type ServiceConfig struct {
	ServiceName string
	Port        int
	Timeout     time.Duration
}

// NewServiceConfig returns a new service config.
func NewServiceConfig(serviceName string, port int, timeout time.Duration) *ServiceConfig {
	return &ServiceConfig{
		ServiceName: serviceName,
		Port:        port,
		Timeout:     timeout,
	}
}
