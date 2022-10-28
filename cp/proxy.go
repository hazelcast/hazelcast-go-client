package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
	"math"
	"time"
)

type proxy struct {
	groupId              types.RaftGroupId
	invocationService    *invocation.Service
	cb                   *cb.CircuitBreaker
	invocationFactory    *cluster.ConnectionInvocationFactory
	logger               logger.LogAdaptor
	objectName           string
	proxyName            string
	serializationService *iserialization.Service
	serviceName          string
}

// Called by proxyManager -> getOrCreateProxy method.
func newProxy(ctx context.Context, bundle *serviceBundle, gi *types.RaftGroupId, svc string, pname string, obj string) (*proxy, error) {
	circuitBreaker := cb.NewCircuitBreaker(
		cb.MaxRetries(math.MaxInt32),
		cb.MaxFailureCount(10),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration((attempt+1)*100) * time.Millisecond
		}))
	p := &proxy{
		groupId:              *gi,
		serviceName:          svc,
		proxyName:            pname,
		objectName:           obj,
		invocationService:    bundle.invocationService,
		serializationService: bundle.serializationService,
		invocationFactory:    bundle.invocationFactory,
		logger:               *bundle.logger,
		cb:                   circuitBreaker,
	}
	return p, nil
}

func (p *proxy) GroupId() types.RaftGroupId {
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

func (p *proxy) tryInvoke(ctx context.Context, f cb.TryHandler) (*proto.ClientMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if res, err := p.cb.TryContext(ctx, f); err != nil {
		return nil, err
	} else {
		return res.(*proto.ClientMessage), nil
	}
}

func (p *proxy) sendInvocation(ctx context.Context, inv invocation.Invocation) error {
	return p.invocationService.SendRequest(ctx, inv)
}

func (p *proxy) invokeOnRandomTarget(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler) (*proto.ClientMessage, error) {
	now := time.Now()
	return p.tryInvoke(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt > 0 {
			request = request.Copy()
		}
		inv := p.invocationFactory.NewInvocationOnRandomTarget(request, handler, now)
		if err := p.sendInvocation(ctx, inv); err != nil {
			return nil, err
		}
		return inv.GetWithContext(ctx)
	})
}
