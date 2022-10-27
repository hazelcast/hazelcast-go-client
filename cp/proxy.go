package cp

import (
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type proxy struct {
	logger               logger.LogAdaptor
	invocationService    *invocation.Service
	serializationService *iserialization.Service
	serviceName          string
	name                 string
}

func newProxy() {
	// Called by proxyManager -> proxyFor method.

}

func (p *proxy) Name() {

}
func (p *proxy) Destroy() {

}
