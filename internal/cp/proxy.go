package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cp/types"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"math"
	"time"
)

type proxy struct {
	logger               logger.LogAdaptor
	cb                   *cb.CircuitBreaker
	serializationService *iserialization.Service
	invocationFactory    *cluster.ConnectionInvocationFactory
	invocationService    *invocation.Service
	objectName           string
	proxyName            string
	serviceName          string
	groupId              types.RaftGroupId
}

func newProxy(bundle *serviceBundle, gi *types.RaftGroupId, svc string, pxy string, obj string) (*proxy, error) {
	circuitBreaker := cb.NewCircuitBreaker(
		cb.MaxRetries(math.MaxInt32),
		cb.MaxFailureCount(10),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration((attempt+1)*100) * time.Millisecond
		}))
	p := &proxy{
		groupId:              *gi,
		serviceName:          svc,
		proxyName:            pxy,
		objectName:           obj,
		invocationService:    bundle.invocationService,
		serializationService: bundle.serializationService,
		invocationFactory:    bundle.invocationFactory,
		logger:               *bundle.logger,
		cb:                   circuitBreaker,
	}
	return p, nil
}

func (p *proxy) Name() string {
	return p.proxyName
}

func (p *proxy) ServiceName() string {
	return p.serviceName
}

func (p *proxy) Destroy(ctx context.Context) error {
	request := codec.EncodeCPGroupDestroyCPObjectRequest(p.groupId, p.serviceName, p.objectName)
	_, err := p.invokeOnRandomTarget(ctx, request, nil)
	return err
}

func (p *proxy) invokeOnRandomTarget(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler) (*proto.ClientMessage, error) {
	now := time.Now()
	if ctx == nil {
		ctx = context.Background()
	}
	if res, err := p.cb.TryContext(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt > 0 {
			request = request.Copy()
		}
		inv := p.invocationFactory.NewInvocationOnRandomTarget(request, handler, now)
		if err := p.invocationService.SendRequest(ctx, inv); err != nil {
			return nil, err
		}
		return inv.GetWithContext(ctx)
	}); err != nil {
		return nil, err
	} else {
		return res.(*proto.ClientMessage), nil
	}
}