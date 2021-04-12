package hazelcast_test

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/it"

	"github.com/hazelcast/hazelcast-go-client"
)

func TestNewClientConfigBuilder(t *testing.T) {
	builder := hazelcast.NewConfigBuilder()
	builder.Cluster().
		SetMembers("192.168.1.1").
		SetName("my-cluster")
	config, err := builder.Config()
	if err != nil {
		t.Error(err)
	}
	if "my-cluster" != config.ClusterConfig.Name {
		t.Errorf("target: %v != %v", "my-cluster", config.ClusterConfig.Name)
	}
}

func TestNewClientWithConfig(t *testing.T) {
	builder := hazelcast.NewConfigBuilder()
	builder.SetClientName("my-client")
	hz := it.MustClient(hazelcast.NewClientWithConfig(builder))
	targetClientName := "my-client"
	if targetClientName != hz.Name() {
		t.Errorf("target: %v != %v", targetClientName, hz.Name())
	}
}
