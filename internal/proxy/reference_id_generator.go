package proxy

import "sync/atomic"

type ReferenceIDGenerator struct {
	nextID int64
}

func NewReferenceIDGenerator() *ReferenceIDGenerator {
	return &ReferenceIDGenerator{nextID: 0}
}

func (gen *ReferenceIDGenerator) NextID() int64 {
	return atomic.AddInt64(&gen.nextID, 1)
}
