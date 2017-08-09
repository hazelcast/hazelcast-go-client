package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
	"math/rand"
	"time"
)

type RandomLoadBalancer struct {
	clusterService *ClusterService
}

func NewRandomLoadBalancer(clusterService *ClusterService) *RandomLoadBalancer {
	rand.Seed(time.Now().Unix())
	return &RandomLoadBalancer{clusterService: clusterService}
}

func (randomLoadBalancer *RandomLoadBalancer) NextAddress() *Address {
	size := len(randomLoadBalancer.clusterService.Members)
	if size > 0 {
		randomIndex := rand.Intn(size)
		member := randomLoadBalancer.clusterService.Members[randomIndex]
		return member.Address()
	}
	return nil
}
