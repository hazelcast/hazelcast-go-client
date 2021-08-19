package hazelcast

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

const invalidFlakeId int64 = -1

type newFlakeIDBatchFn func(ctx context.Context, f *FlakeIdGenerator) (flakeIdBatch, error)

type FlakeIdGenerator struct {
	*proxy
	mu         *sync.Mutex
	batch      atomic.Value
	newBatchFn newFlakeIDBatchFn
	config     FlakeIDGeneratorConfig
}

func newFlakeIdGenerator(p *proxy, config FlakeIDGeneratorConfig, newBatchFn newFlakeIDBatchFn) *FlakeIdGenerator {
	f := &FlakeIdGenerator{
		proxy:      p,
		mu:         &sync.Mutex{},
		batch:      atomic.Value{},
		newBatchFn: newBatchFn,
		config:     config,
	}
	f.batch.Store(flakeIdBatch{})
	return f
}

func (f *FlakeIdGenerator) NewId(ctx context.Context) (int64, error) {
	for {
		batch := f.batch.Load().(flakeIdBatch)
		id := batch.next()
		if id != invalidFlakeId {
			return id, nil
		}
		f.mu.Lock()
		if batch != f.batch.Load().(flakeIdBatch) {
			f.mu.Unlock()
			continue
		}
		if b, err := f.newBatchFn(ctx, f); err != nil {
			f.mu.Unlock()
			return invalidFlakeId, err
		} else {
			f.batch.Store(b)
			f.mu.Unlock()
		}
	}
}

func defaultNewFlakeIDBatchFn(ctx context.Context, f *FlakeIdGenerator) (flakeIdBatch, error) {
	request := codec.EncodeFlakeIdGeneratorNewIdBatchRequest(f.name, f.config.PrefetchCount)
	resp, err := f.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return flakeIdBatch{}, err
	}
	base, inc, size := codec.DecodeFlakeIdGeneratorNewIdBatchResponse(resp)
	return flakeIdBatch{
		base:      base,
		increment: inc,
		size:      size,
		index:     new(int32),
		expiresAt: time.Now().Add(time.Duration(f.config.PrefetchExpiration)),
	}, nil
}

type flakeIdBatch struct {
	base      int64
	increment int64
	size      int32
	index     *int32
	expiresAt time.Time
}

func (f *flakeIdBatch) next() int64 {
	if time.Now().After(f.expiresAt) {
		return invalidFlakeId
	}
	var idx int32
	for {
		idx = atomic.LoadInt32(f.index)
		if idx == f.size {
			return invalidFlakeId
		}
		if atomic.CompareAndSwapInt32(f.index, idx, idx+1) {
			return f.base + int64(idx)*f.increment
		}
	}
}
