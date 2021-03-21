package proxy

import "sync/atomic"

type ReferenceIDGenerator interface {
	NextID() int64
}

type ReferenceIDGeneratorImpl struct {
	nextID int64
}

func NewReferenceIDGeneratorImpl() *ReferenceIDGeneratorImpl {
	return &ReferenceIDGeneratorImpl{nextID: 0}
}

func (gen *ReferenceIDGeneratorImpl) NextID() int64 {
	return atomic.AddInt64(&gen.nextID, 1)
}
