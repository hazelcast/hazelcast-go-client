package internal

import "sync/atomic"

type ProxyManager struct {
	ReferenceId int64
	client      *HazelcastClient
}

func newProxyManager(client *HazelcastClient) *ProxyManager {
	return &ProxyManager{
		ReferenceId: 0,
		client:      client,
	}
}
func (proxyManager *ProxyManager) nextReferenceId() int64 {
	return atomic.AddInt64(&proxyManager.ReferenceId, 1)
}
