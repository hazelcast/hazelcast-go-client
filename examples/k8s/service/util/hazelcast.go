package util

import (
	"context"
	"fmt"
	"os"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

// HazelcastClientInfo contains info about client
type HazelcastClientInfo struct {
	Name    string
	Running bool
	MapSize int
}

// NewHazelcastClient returns Hazelcast client instance with default config.
func NewHazelcastClient(ctx context.Context) (*hazelcast.Client, error) {
	var err error
	cfg := hazelcast.Config{
		ClientName: "proxy-service-go-client",
	}
	cfg.Logger.Level = logger.ErrorLevel
	cc := &cfg.Cluster
	// Unisocket network configuration is not a mandatory setting.
	cc.Unisocket = true
	_, locally := os.LookupEnv(WithoutK8s)
	if locally {
		cc.Network.SetAddresses(fmt.Sprintf("%s:%s", "localhost", "5701"))
	} else {
		cc.Network.SetAddresses(fmt.Sprintf("%s:%s", "hazelcast-sample.default.svc", "5701"))
	}
	var client *hazelcast.Client
	if client, err = hazelcast.StartNewClientWithConfig(ctx, cfg); err != nil {
		return nil, err
	}
	return client, nil
}

// ExampleMap creates a sample map which contains example entries.
func ExampleMap(ctx context.Context, c *hazelcast.Client, name string) (m *hazelcast.Map, err error) {
	if m, err = c.GetMap(ctx, name); err != nil {
		return nil, err
	}
	if err = m.PutAll(ctx, ExampleMapEntries...); err != nil {
		return nil, err
	}
	return m, nil
}
