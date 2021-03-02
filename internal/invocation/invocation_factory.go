package invocation

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

type Factory interface {
	NewInvocationOnPartitionOwner(message *proto.ClientMessage, partitionID int32) Invocation
	NewInvocationOnRandomTarget(message *proto.ClientMessage) Invocation
	NewInvocationOnKeyOwner(message *proto.ClientMessage, data serialization.Data) Invocation
	NewInvocationOnTarget(message *proto.ClientMessage, address *core.Address) Invocation
}
