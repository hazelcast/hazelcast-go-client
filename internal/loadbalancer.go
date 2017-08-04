package internal


import (
	. "github.com/hazelcast/go-client/internal/protocol"
	"math/rand"
	"time"
)

type RandomLoadBalancer struct {
	clusterService *ClusterService
}
func (randomLoadBalancer *RandomLoadBalancer) NextAddress() *Address {
	rand.Seed(time.Now().Unix())
	return randomLoadBalancer.clusterService.members[rand.Intn(len(randomLoadBalancer.clusterService.members))].Address()
}