package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
)
type ClusterService struct {
	client *HazelcastClient
	Members []Member
	ownerUuid string
	uuid string
}

func NewClusterService(client *HazelcastClient) *ClusterService {
	return &ClusterService{client:client}
}