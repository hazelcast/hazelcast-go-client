package invocation

import (
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	serialization "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

type MessageKeyData struct {
	Message *proto.ClientMessage
	KeyData serialization.Data
}

type Factory interface {
	NewInvocationOnPartitionOwner(message *proto.ClientMessage, partitionID int32) Invocation
	NewInvocationOnRandomTarget(message *proto.ClientMessage, messageHandler proto.ClientMessageHandler) Invocation
	NewInvocationOnKeyOwner(message *proto.ClientMessage, data serialization.Data) Invocation
	NewInvocationOnTarget(message *proto.ClientMessage, address pubcluster.Address) Invocation
}
