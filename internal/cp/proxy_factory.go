package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cp/types"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"strings"
	"time"
)

const (
	atomicLongService      = "hz:raft:atomicLongService"
	atomicReferenceService = "hz:raft:atomicRefService"
	countDownLatchService  = "hz:raft:countDownLatchService"
	lockService            = "hz:raft:lockService"
	semaphoreService       = "hz:raft:semaphoreService"
)

const (
	defaultGroupName    = "default"
	metadataCpGroupName = "metadata"
)

type serviceBundle struct {
	invocationService    *invocation.Service
	serializationService *iserialization.Service
	invocationFactory    *cluster.ConnectionInvocationFactory
	logger               *logger.LogAdaptor
}

func (b serviceBundle) check() {
	if b.invocationService == nil {
		panic("invocationFactory is nil")
	}
	if b.invocationFactory == nil {
		panic("invocationService is nil")
	}
	if b.serializationService == nil {
		panic("serializationService is nil")
	}
	if b.logger == nil {
		panic("logger is nil")
	}
}

type proxyFactory struct {
	bundle *serviceBundle
}

func newCpProxyFactory(ss *iserialization.Service, cif *cluster.ConnectionInvocationFactory, is *invocation.Service, l *logger.LogAdaptor) *proxyFactory {
	b := &serviceBundle{
		invocationService:    is,
		invocationFactory:    cif,
		serializationService: ss,
		logger:               l,
	}
	b.check()
	p := &proxyFactory{
		bundle: b,
	}
	return p
}

func (m *proxyFactory) getOrCreateProxy(ctx context.Context, serviceName string, p string, wrapProxyFn func(p *proxy) (interface{}, error)) (interface{}, error) {
	p, err := withoutDefaultGroupName(p)
	if err != nil {
		return nil, err
	}
	o, err := objectNameForProxy(p)
	if err != nil {
		return nil, err
	}
	g, err := m.createGroupId(ctx, p)
	if err != nil {
		return nil, err
	}
	prxy, err := newProxy(m.bundle, g, serviceName, p, o)
	if err != nil {
		return nil, err
	}
	wprxy, err := wrapProxyFn(prxy)
	if err != nil {
		return nil, err
	}
	return wprxy, nil
}

func objectNameForProxy(name string) (string, error) {
	idx := strings.Index(name, "@")
	if idx == -1 {
		return name, nil
	}
	groupName := strings.TrimSpace(name[idx+1:])
	if len(groupName) <= 0 {
		return "", hzerrors.NewIllegalArgumentError("Custom CP group name cannot be empty string", nil)
	}
	objectName := strings.TrimSpace(name[:idx])
	if len(objectName) <= 0 {
		return "", hzerrors.NewIllegalArgumentError("Object name cannot be empty string", nil)
	}
	return objectName, nil
}

func (m *proxyFactory) createGroupId(ctx context.Context, proxyName string) (*types.RaftGroupId, error) {
	request := codec.EncodeCPGroupCreateCPGroupRequest(proxyName)
	now := time.Now()
	inv := m.bundle.invocationFactory.NewInvocationOnRandomTarget(request, nil, now)
	err := m.bundle.invocationService.SendRequest(context.Background(), inv)
	if err != nil {
		return nil, err
	}
	response, err := inv.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	groupId := codec.DecodeCPGroupCreateCPGroupResponse(response)
	return &groupId, nil
}

func withoutDefaultGroupName(proxyName string) (string, error) {
	name := strings.TrimSpace(proxyName)
	idx := strings.Index(name, "@")
	if idx == -1 {
		return name, nil
	}
	if ci := strings.Index(name[idx+1:], "@"); ci != -1 {
		return "", hzerrors.NewIllegalArgumentError("Custom group name must be specified at most once", nil)
	}
	groupName := strings.TrimSpace(name[idx+1:])
	if lgn := strings.ToLower(groupName); lgn == metadataCpGroupName {
		return "", hzerrors.NewIllegalArgumentError("CP data structures cannot run on the METADATA CP group!", nil)
	}
	if strings.ToLower(groupName) == defaultGroupName {
		return name[:idx], nil
	}
	return name, nil
}

func (m *proxyFactory) getAtomicLong(ctx context.Context, name string) (*AtomicLong, error) {
	p, err := m.getOrCreateProxy(ctx, atomicLongService, name, func(p *proxy) (interface{}, error) {
		return newAtomicLong(p), nil
	})
	if err != nil {
		return nil, err
	}
	return p.(*AtomicLong), nil
}
