package client

import "github.com/hazelcast/hazelcast-go-client/v4/internal/proxy"

type Client interface {
	Name() string
	GetMap(name string) (proxy.Map, error)
}
