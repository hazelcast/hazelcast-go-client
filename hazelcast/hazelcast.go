package hazelcast

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/config"
)

// NewClient creates and returns a new Client.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewClient() (Client, error) {
	return NewClientWithConfig(config.New())
}

// NewClientWithConfig creates and returns a new Client with the given config.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewClientWithConfig(config *config.Config) (Client, error) {
	return internal.NewHazelcastClient(config)
}

// NewConfig creates and returns a new config.
func NewConfig() *config.Config {
	return config.New()
}
