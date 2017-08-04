package hazelcast

import (
	. "github.com/hazelcast/go-client/config"
	"github.com/hazelcast/go-client/internal"
)

func NewHazelcastClient() IHazelcastInstance {
	return NewHazelcastClientWithConfig(NewClientConfig())
}

func NewHazelcastClientWithConfig(config ClientConfig) IHazelcastInstance {
	return internal.NewHazelcastClient(config)
}

type IMap interface {
	Put(key interface{}, value interface{}) (oldValue interface{}, err error)
	Get(key interface{}) (value interface{}, err error)
}

type IHazelcastInstance interface {
	GetMap(name string) IMap
}
