package cp

import (
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type proxy struct {
	groupId              RaftGroupId
	invocationService    *invocation.Service
	logger               logger.LogAdaptor
	objectName           string
	proxyName            string
	serializationService *iserialization.Service
	serviceName          string
}

func newProxy() {
	// Called by proxyManager -> proxyFor method.

}

func (p *proxy) GroupId() RaftGroupId {
	return p.groupId
}

func (p *proxy) Name() string {
	return p.proxyName
}

func (p *proxy) ServiceName() string {
	return p.serviceName
}

func (p *proxy) Destroy() {

}
