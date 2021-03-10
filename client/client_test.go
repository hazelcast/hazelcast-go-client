package client_test

import (
	"github.com/hazelcast/hazelcast-go-client/v4/client"
	"testing"
)

func TestNewClientConfigBuilder(t *testing.T) {
	builder := client.NewClientConfigBuilder()
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
	hz := client.NewClient()
	targetName := "hz.client_1"
	if targetName != hz.Name() {
		t.Errorf("target: %v != %v", targetName, hz.Name())
		return
	}
}

func TestNewClientWithConfig(t *testing.T) {
	builder := client.NewClientConfigBuilder()
	builder.SetClientName("my-client")
	config, err := builder.Config()
	if err != nil {
		t.Error(err)
		return
	}
	hz := client.NewClientWithConfig(config)
	targetClientName := "my-client"
	if targetClientName != hz.Name() {
		t.Errorf("target: %v != %v", targetClientName, hz.Name())
	}
}
