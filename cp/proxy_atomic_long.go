package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

type AtomicLong struct {
	*proxy
}

func newAtomicLong(p *proxy) *AtomicLong {
	return &AtomicLong{proxy: p}
}

func (al *AtomicLong) AddAndGet(delta int64) (int64, error) {
	request := codec.EncodeAtomicLongAddAndGetRequest(al.groupId, al.proxyName, delta)
	response, err := al.invokeOnRandomTarget(context.Background(), request, nil)
	if err != nil {
		return -1, err
	}
	return codec.DecodeAtomicLongAddAndGetResponse(response), nil
}

func (al *AtomicLong) CompareAndSet(expect int64, update int64) (bool, error) {
	request := codec.EncodeAtomicLongCompareAndSetRequest(al.groupId, al.proxyName, expect, update)
	response, err := al.invokeOnRandomTarget(context.Background(), request, nil)
	if err != nil {
		return false, err
	}
	return codec.DecodeAtomicLongCompareAndSetResponse(response), nil
}

func (al *AtomicLong) Get() (interface{}, error) {
	request := codec.EncodeAtomicLongGetRequest(al.groupId, al.proxyName)
	if response, err := al.invokeOnRandomTarget(context.Background(), request, nil); err != nil {
		return nil, err
	} else {
		return codec.DecodeAtomicLongGetResponse(response), nil
	}
}

func (al *AtomicLong) GetAndAdd(ctx context.Context, delta int64) (int64, error) {
	request := codec.EncodeAtomicLongGetAndAddRequest(al.groupId, al.proxyName, delta)
	response, err := al.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return -1, err
	}
	return codec.DecodeAtomicLongGetAndAddResponse(response), nil
}

func (al *AtomicLong) GetAndSet(ctx context.Context, value int64) (int64, error) {
	request := codec.EncodeAtomicLongGetAndSetRequest(al.groupId, al.proxyName, value)
	response, err := al.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return -1, err
	}
	return codec.DecodeAtomicLongGetAndSetResponse(response), nil
}

func (al *AtomicLong) Set(ctx context.Context, value int64) error {
	request := codec.EncodeAtomicLongGetAndSetRequest(al.groupId, al.proxyName, value)
	_, err := al.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return err
	}
	return nil
}

func (al *AtomicLong) IncrementAndGet() (int64, error) {
	return al.AddAndGet(1)
}

func (al *AtomicLong) DecrementAndGet() (int64, error) {
	return al.AddAndGet(-1)
}

func (al *AtomicLong) GetAndDecrement(ctx context.Context) (int64, error) {
	return al.GetAndAdd(ctx, -1)
}

func (al *AtomicLong) GetAndIncrement(ctx context.Context) (int64, error) {
	return al.GetAndAdd(ctx, 1)
}
