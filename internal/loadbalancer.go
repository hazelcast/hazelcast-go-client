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
	membersList := randomLoadBalancer.clusterService.GetMemberList()
	size := len(membersList)
	if size > 0 {
		randomIndex := rand.Intn(size)
		member := membersList[randomIndex]
		return member.Address().(*Address)
	}
	return nil
}
