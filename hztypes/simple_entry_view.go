package hztypes

import (
	"github.com/hazelcast/hazelcast-go-client/internal/types"
)

type SimpleEntryView = types.SimpleEntryView

func NewSimpleEntryView(key, value interface{}, cost, creationTime, expirationTime, hits, lastAccessTime,
	lastStoredTime, lastUpdateTime, version, ttl, maxIdle int64) *SimpleEntryView {
	return types.NewSimpleEntryView(key, value, cost, creationTime, expirationTime, hits, lastAccessTime, lastStoredTime, lastUpdateTime, version, ttl, maxIdle)
}
