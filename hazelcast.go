package hazelcast

import (
	"github.com/hazelcast/go-client/config"
	"github.com/hazelcast/go-client/core"
	"github.com/hazelcast/go-client/internal"
)

func NewHazelcastClient() (IHazelcastInstance, error) {
	return NewHazelcastClientWithConfig(config.NewClientConfig())
}

func NewHazelcastClientWithConfig(config *config.ClientConfig) (IHazelcastInstance, error) {
	return internal.NewHazelcastClient(config)
}

func NewHazelcastConfig() *config.ClientConfig {
	return config.NewClientConfig()
}

type IHazelcastInstance interface {
	GetMap(name string) (core.IMap, error)
	GetDistributedObject(serviceName string, name string) (core.IDistributedObject, error)
	Shutdown()
	GetCluster() core.ICluster
	GetLifecycle() core.ILifecycle
}
