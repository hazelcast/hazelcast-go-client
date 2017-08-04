package internal

import (
	."github.com/hazelcast/go-client/config"
	. "github.com/hazelcast/go-client/internal/protocol"
)
type ClusterService struct {
	config ClientConfig
	client HazelcastClient
	members []Member
	ownerConnectionAddress Address
	ownerUuid string
	uuid string
}