package portrangeutil

import (
	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetAddresses(t *testing.T) {
	host := "127.0.0.1"
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}
	expectedAddrs := []pubcluster.Address{
		pubcluster.NewAddress(host, 5701),
		pubcluster.NewAddress(host, 5702),
		pubcluster.NewAddress(host, 5703),
	}
	addrs := GetAddresses(host, portRange)
	assert.Equal(t, addrs, expectedAddrs)
}
