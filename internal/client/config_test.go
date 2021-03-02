package client_test

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/client"
	"reflect"
	"testing"
)

func TestConfig(t *testing.T) {
	configBuilder := client.NewConfigBuilderImpl()
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
	if !reflect.DeepEqual([]string{"192.168.1.2"}, config.Network.Addresses) {
		t.Errorf("target: %v != %v", []string{"192.168.1.2"}, config.ClusterName)
	}

}
