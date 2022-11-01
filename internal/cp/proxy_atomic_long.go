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

func (a *AtomicLong) AddAndGet(ctx context.Context, delta int64) (int64, error) {
	request := codec.EncodeAtomicLongAddAndGetRequest(a.groupId, a.proxyName, delta)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return -1, err
	} else {
		return codec.DecodeAtomicLongAddAndGetResponse(response), nil
	}
}

func (a *AtomicLong) CompareAndSet(ctx context.Context, expect int64, update int64) (bool, error) {
	request := codec.EncodeAtomicLongCompareAndSetRequest(a.groupId, a.proxyName, expect, update)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return false, err
	} else {
		return codec.DecodeAtomicLongCompareAndSetResponse(response), nil
	}
}

func (a *AtomicLong) Get(ctx context.Context) (interface{}, error) {
	request := codec.EncodeAtomicLongGetRequest(a.groupId, a.proxyName)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return nil, err
	} else {
		return codec.DecodeAtomicLongGetResponse(response), nil
	}
}

func (a *AtomicLong) GetAndAdd(ctx context.Context, delta int64) (int64, error) {
	request := codec.EncodeAtomicLongGetAndAddRequest(a.groupId, a.proxyName, delta)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return -1, err
	} else {
		return codec.DecodeAtomicLongGetAndAddResponse(response), nil
	}
}

func (a *AtomicLong) GetAndSet(ctx context.Context, value int64) (int64, error) {
	request := codec.EncodeAtomicLongGetAndSetRequest(a.groupId, a.proxyName, value)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return -1, err
	} else {
		return codec.DecodeAtomicLongGetAndSetResponse(response), nil
	}
}

func (a *AtomicLong) Set(ctx context.Context, value int64) error {
	request := codec.EncodeAtomicLongGetAndSetRequest(a.groupId, a.proxyName, value)
	_, err := a.invokeOnRandomTarget(ctx, request, nil)
	return err
}

func (a *AtomicLong) Apply(ctx context.Context, function interface{}) (interface{}, error) {
	data, _ := a.serializationService.ToData(function)
	request := codec.EncodeAtomicLongApplyRequest(a.groupId, a.objectName, data)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return nil, err
	} else {
		obj, _ := a.serializationService.ToObject(codec.DecodeAtomicLongApplyResponse(response))
		return obj, nil
	}
}

func (a *AtomicLong) IncrementAndGet(ctx context.Context) (int64, error) {
	return a.AddAndGet(ctx, 1)
}

func (a *AtomicLong) DecrementAndGet(ctx context.Context) (int64, error) {
	return a.AddAndGet(ctx, -1)
}

func (a *AtomicLong) GetAndDecrement(ctx context.Context) (int64, error) {
	return a.GetAndAdd(ctx, -1)
}

func (a *AtomicLong) GetAndIncrement(ctx context.Context) (int64, error) {
	return a.GetAndAdd(ctx, 1)
}
