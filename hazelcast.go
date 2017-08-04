package hazelcast

import "github.com/hazelcast/go-client/internal"

func NewHazelcastClient() IHazelcastInstance {
	return internal.NewHazelcastClient(NewClientConfig())
}

func NewHazelcastClientWithConfig(config ClientConfig) IHazelcastInstance {
	return internal.NewHazelcastClient(config)
}

type IMap interface {

}

type IHazelcastInstance interface {

	GetMap(name string) IMap

}
