package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
)

func TestRemoveAddr(t *testing.T) {
	cm := newConnectionMap(pubcluster.NewRoundRobinLoadBalancer())
	conn1 := &Connection{connectionID: 1}
	conn2 := &Connection{connectionID: 2}
	conn3 := &Connection{connectionID: 3}
	cm.removeAddr("1.2.3.4:5600")
	assert.Equal(t, 0, len(cm.addrs))
	cm.AddConnection(conn1, "100.200.300.400:5678")
	cm.AddConnection(conn2, "100.200.300.401:5678")
	cm.AddConnection(conn3, "100.200.300.402:5678")
	if !assert.Equal(t, []pubcluster.Address{"100.200.300.400:5678", "100.200.300.401:5678", "100.200.300.402:5678"}, cm.addrs) {
		t.FailNow()
	}
	cm.removeAddr("100.200.300.401:5678")
	if !assert.Equal(t, []pubcluster.Address{"100.200.300.400:5678", "100.200.300.402:5678"}, cm.addrs) {
		t.FailNow()
	}
	cm.removeAddr("100.200.300.402:5678")
	if !assert.Equal(t, []pubcluster.Address{"100.200.300.400:5678"}, cm.addrs) {
		t.FailNow()
	}
	cm.removeAddr("100.200.300.400:5678")
	if !assert.Equal(t, []pubcluster.Address{}, cm.addrs) {
		t.FailNow()
	}
}
