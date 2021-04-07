package hazelcast_test

import (
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
)

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
