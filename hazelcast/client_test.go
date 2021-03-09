package hazelcast_test

import (
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"testing"
)

func TestNewClientConfigBuilder(t *testing.T) {
	builder := hazelcast.NewClientConfigBuilder()
	builder.SetClusterName("my-cluster")
	builder.Network().SetAddresses("192.168.1.1")
	config, err := builder.Config()
	if err != nil {
		t.Error(err)
	}
	if "my-cluster" != config.ClusterName {
		t.Errorf("target: %v != %v", "my-cluster", config.ClusterName)
	}
}

func TestNewClient(t *testing.T) {
	client := hazelcast.NewClient()
	targetName := "hz.client_1"
	if targetName != client.Name() {
		t.Errorf("target: %v != %v", targetName, client.Name())
		return
	}
}

func TestNewClientWithConfig(t *testing.T) {
	builder := hazelcast.NewClientConfigBuilder()
	builder.SetClientName("my-client")
	config, err := builder.Config()
	if err != nil {
		t.Error(err)
		return
	}
	client := hazelcast.NewClientWithConfig(config)
	targetClientName := "my-client"
	if targetClientName != client.Name() {
		t.Errorf("target: %v != %v", targetClientName, client.Name())
	}
}
