package hazelcast_test

import (
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"reflect"
	"testing"
)

func TestConfig(t *testing.T) {
	configBuilder := hazelcast.NewClientConfigBuilder()
	configBuilder.SetClusterName("my-cluster")
	configBuilder.Network().SetAddresses("192.168.1.2")
	config, err := configBuilder.Config()
	if err != nil {
		t.Error(err)
		return
	}
	if "my-cluster" != config.ClusterName {
		t.Errorf("target: my-cluster != %v", config.ClusterName)
	}
	targetAddrs := []string{"192.168.1.2"}
	if !reflect.DeepEqual(targetAddrs, config.Network.Addrs()) {
		t.Errorf("target: %v != %v", targetAddrs, config.Network.Addrs())
	}
}
