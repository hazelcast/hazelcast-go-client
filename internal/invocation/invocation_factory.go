package invocation

import (
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

type MessageKeyData struct {
	Message *proto.ClientMessage
	KeyData serialization.Data
}

type Factory interface {
	NewInvocationOnPartitionOwner(message *proto.ClientMessage, partitionID int32) Invocation
	NewInvocationOnRandomTarget(message *proto.ClientMessage) Invocation
	NewInvocationOnKeyOwner(message *proto.ClientMessage, data serialization.Data) Invocation
	NewInvocationOnTarget(message *proto.ClientMessage, address pubcluster.Address) Invocation
}
