package portrangeutil

import (
	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
)

func GetAddresses(host string, portRange pubcluster.PortRange) []pubcluster.Address {
	var addrs []pubcluster.Address
	for i := portRange.Min; i <= portRange.Max; i++ {
		addrs = append(addrs, pubcluster.NewAddress(host, int32(i)))
	}
	return addrs
}
