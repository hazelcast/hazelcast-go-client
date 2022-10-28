package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/types"
	"strings"
	"sync"
	"time"
)

type ProxyManager struct {
	bundle  CpCreationBundle
	mu      *sync.RWMutex
	proxies map[string]interface{}
}

func newCpProxyManager(bundle CpCreationBundle) (*ProxyManager, error) {
	p := &ProxyManager{
		mu:      &sync.RWMutex{},
		proxies: map[string]interface{}{},
		bundle:  bundle,
	}
	return p, nil
}

func (m *ProxyManager) proxyFor(
	ctx context.Context,
	serviceName string,
	proxyName string,
	wrapProxyFn func(p *proxy) (interface{}, error)) (interface{}, error) {
	// identifiers for proxy
	proxyName = m.withoutDefaultGroupName(ctx, proxyName)
	objectName := m.objectNameForProxy(ctx, proxyName)
	groupId, _ := m.createGroupId(ctx, proxyName)
	m.mu.RLock()
	wrapper, ok := m.proxies[proxyName]
	m.mu.RUnlock()
	if ok {
		return wrapper, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if wrapper, ok := m.proxies[proxyName]; ok {
		// someone has already created the proxy
		return wrapper, nil
	}
	p, err := newProxy(ctx, m.bundle, groupId, serviceName, proxyName, objectName)
	if err != nil {
		return nil, err
	}
	wrapper, err = wrapProxyFn(p)
	if err != nil {
		return nil, err
	}
	m.proxies[proxyName] = wrapper
	return wrapper, nil
}

func (pm *ProxyManager) objectNameForProxy(ctx context.Context, name string) string {
	idx := strings.Index(name, "@")
	if idx == -1 {
		return name
	}
	groupName := strings.TrimSpace(name[idx+1:])
	if len(groupName) <= 0 {
		panic("Custom CP group name cannot be empty string")
	}
	objectName := strings.TrimSpace(name[:idx])
	if len(objectName) <= 0 {
		panic("Object name cannot be empty string")
	}
	return objectName
}

func (pm *ProxyManager) createGroupId(ctx context.Context, proxyName string) (*types.RaftGroupId, error) {
	request := codec.EncodeCPGroupCreateCPGroupRequest(proxyName)
	response, err := pm.invoke(ctx, request)
	if err != nil {
		return nil, err
	}
	groupId := codec.DecodeCPGroupCreateCPGroupResponse(response)
	return &groupId, nil
}

const (
	_DEFAULT_GROUP_NAME     = "default"
	_METADATA_CP_GROUP_NAME = "metadata"
)

func (pm *ProxyManager) withoutDefaultGroupName(ctx context.Context, proxyName string) string {
	name := strings.TrimSpace(proxyName)
	idx := strings.Index(name, "@")
	if idx == -1 {
		return name
	}
	if ci := strings.Index(name[idx+1:], "@"); ci != -1 {
		panic("Custom group name must be specified at most once")
	}
	groupName := strings.TrimSpace(name[idx+1:])
	if lgn := strings.ToLower(groupName); lgn == _METADATA_CP_GROUP_NAME {
		panic("\"CP data structures cannot run on the METADATA CP group!\"")
	}
	if strings.ToLower(groupName) == _DEFAULT_GROUP_NAME {
		return name[:idx]
	}
	return name

}

func (pm *ProxyManager) invoke(ctx context.Context, request *proto.ClientMessage) (*proto.ClientMessage, error) {
	now := time.Now()
	inv := pm.bundle.InvocationFactory.NewInvocationOnRandomTarget(request, nil, now)
	err := pm.bundle.InvocationService.SendRequest(context.Background(), inv)
	if err != nil {
		return nil, err
	}
	return inv.GetWithContext(ctx)
}

func (pm *ProxyManager) getAtomicLong(ctx context.Context, name string) (*AtomicLong, error) {
	p, err := pm.proxyFor(ctx, ATOMIC_LONG_SERVICE, name, func(p *proxy) (interface{}, error) {
		return newAtomicLong(p), nil
	})
	if err != nil {
		return nil, err
	}
	return p.(*AtomicLong), nil
}
