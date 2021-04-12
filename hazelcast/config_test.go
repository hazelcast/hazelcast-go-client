package hazelcast_test

import (
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/it"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
)

func TestDefaultConfig(t *testing.T) {
	configBuilder := hazelcast.NewClientConfigBuilder()
	if config, err := configBuilder.Config(); err != nil {
		t.Fatal(err)
	} else {
		it.AssertEquals(t, config.LoggerConfig.Level, logger.InfoLevel)
		it.AssertEquals(t, config.ClusterConfig.Name, "dev")
		it.AssertEquals(t, config.ClusterConfig.Addrs, []string{"localhost:5701"})
	}
}

func TestConfig(t *testing.T) {
	configBuilder := hazelcast.NewClientConfigBuilder()
	configBuilder.Cluster().
		SetMembers("192.168.1.2").
		SetName("my-cluster")
	config, err := configBuilder.Config()
	if err != nil {
		t.Error(err)
		return
	}
	if "my-cluster" != config.ClusterConfig.Name {
		t.Errorf("target: my-cluster != %v", config.ClusterConfig.Name)
	}
	targetAddrs := []string{"192.168.1.2"}
	if !reflect.DeepEqual(targetAddrs, config.ClusterConfig.Addrs) {
		t.Errorf("target: %v != %v", targetAddrs, config.ClusterConfig.Addrs)
	}
}
