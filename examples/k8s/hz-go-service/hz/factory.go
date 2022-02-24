package hz

import (
	"context"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

const localTest = false

// ClientInfo contains info about client
type ClientInfo struct {
	ClientName    string `json:"clientName"`
	ClientRunning bool   `json:"clientRunning"`
	MapSize       int    `json:"mapSize"`
}

// NewHzClient Return hazelcast client instance with default config.
func NewHzClient(ctx context.Context) (*hazelcast.Client, error) {
	config := hazelcast.Config{
		ClientName: "hz-go-service-client",
	}
	cc := &config.Cluster
	if localTest {
		cc.Network.SetAddresses(fmt.Sprintf("%s:%s", "localhost", "5701"))
	} else {
		cc.Network.SetAddresses(fmt.Sprintf("%s:%s", "hazelcast.default.svc", "5701"))
	}
	cc.Discovery.UsePublicIP = false
	cc.Unisocket = true
	config.Logger.Level = logger.InfoLevel
	client, err := NewHzClientWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// NewHzClientWithConfig Return new hazelcast client instance with given config.
func NewHzClientWithConfig(ctx context.Context, config hazelcast.Config) (*hazelcast.Client, error) {
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	return client, nil
}
