package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

// AtomicLong is a redundant and highly available distributed counter
// for 64-bit integers (``long`` type in Java).
// It works on top of the Raft consensus algorithm. It offers linearizability
// during crash failures and network partitions. It is CP with respect to
// the CAP principle. If a network partition occurs, it remains available
// on at most one side of the partition.
// AtomicLong implementation does not offer exactly-once / effectively-once
// execution semantics. It goes with at-least-once execution semantics
// by default and can cause an API call to be committed multiple times
// in case of CP member failures. It can be tuned to offer at-most-once
// execution semantics. Please see `fail-on-indeterminate-operation-state`
// server-side setting.
type AtomicLong struct {
	*proxy
}

func newAtomicLong(p *proxy) *AtomicLong {
	return &AtomicLong{proxy: p}
}

// AddAndGet atomically adds the given value to the current value.
func (a AtomicLong) AddAndGet(ctx context.Context, delta int64) (int64, error) {
	request := codec.EncodeAtomicLongAddAndGetRequest(a.groupId, a.proxyName, delta)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return 0, err
	} else {
		return codec.DecodeAtomicLongAddAndGetResponse(response), nil
	}
}

// CompareAndSet Atomically sets the value to the given updated value only if the current value equals the expected value.
func (a AtomicLong) CompareAndSet(ctx context.Context, expect int64, update int64) (bool, error) {
	request := codec.EncodeAtomicLongCompareAndSetRequest(a.groupId, a.proxyName, expect, update)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return false, err
	} else {
		return codec.DecodeAtomicLongCompareAndSetResponse(response), nil
	}
}

// Get gets the current value.
func (a AtomicLong) Get(ctx context.Context) (int64, error) {
	request := codec.EncodeAtomicLongGetRequest(a.groupId, a.proxyName)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return 0, err
	} else {
		return codec.DecodeAtomicLongGetResponse(response), nil
	}
}

// GetAndAdd atomically adds the given value to the current value.
func (a AtomicLong) GetAndAdd(ctx context.Context, delta int64) (int64, error) {
	request := codec.EncodeAtomicLongGetAndAddRequest(a.groupId, a.proxyName, delta)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return 0, err
	} else {
		return codec.DecodeAtomicLongGetAndAddResponse(response), nil
	}
}

// GetAndSet atomically sets the given value and returns the old value.
func (a AtomicLong) GetAndSet(ctx context.Context, value int64) (int64, error) {
	request := codec.EncodeAtomicLongGetAndSetRequest(a.groupId, a.proxyName, value)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return 0, err
	} else {
		return codec.DecodeAtomicLongGetAndSetResponse(response), nil
	}
}

// Set atomically sets the given value.
func (a AtomicLong) Set(ctx context.Context, value int64) error {
	request := codec.EncodeAtomicLongGetAndSetRequest(a.groupId, a.proxyName, value)
	_, err := a.invokeOnRandomTarget(ctx, request, nil)
	return err
}

// Apply applies a function on the value, the actual stored value will not change.
func (a AtomicLong) Apply(ctx context.Context, function interface{}) (interface{}, error) {
	data, _ := a.serializationService.ToData(function)
	request := codec.EncodeAtomicLongApplyRequest(a.groupId, a.objectName, data)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return nil, err
	} else {
		obj, _ := a.serializationService.ToObject(codec.DecodeAtomicLongApplyResponse(response))
		return obj, nil
	}
}

// Alter alters the currently stored value by applying a function on it.
func (a AtomicLong) Alter(ctx context.Context, function interface{}) error {
	data, err := a.serializationService.ToData(function)
	if err != nil {
		return err
	}
	request := codec.EncodeAtomicLongAlterRequest(a.groupId, a.objectName, data, 1)
	_, err = a.invokeOnRandomTarget(ctx, request, nil)
	return err
}

// GetAndAlter alters the currently stored value by applying a function on it and gets the old value.
func (a AtomicLong) GetAndAlter(ctx context.Context, function interface{}) (int64, error) {
	data, err := a.serializationService.ToData(function)
	if err != nil {
		return 0, err
	}
	request := codec.EncodeAtomicLongAlterRequest(a.groupId, a.objectName, data, 0)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return 0, err
	} else {
		return codec.DecodeAtomicLongAlterResponse(response), nil
	}
}

// AlterAndGet alters the currently stored value by applying a function on it and gets the result.
func (a AtomicLong) AlterAndGet(ctx context.Context, function interface{}) (int64, error) {
	data, err := a.serializationService.ToData(function)
	if err != nil {
		return 0, err
	}
	request := codec.EncodeAtomicLongAlterRequest(a.groupId, a.objectName, data, 1)
	if response, err := a.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return 0, err
	} else {
		return codec.DecodeAtomicLongAlterResponse(response), nil
	}
}

// IncrementAndGet atomically increments the current value by one.
func (a AtomicLong) IncrementAndGet(ctx context.Context) (int64, error) {
	return a.AddAndGet(ctx, 1)
}

// DecrementAndGet atomically decrements the current value by one.
func (a AtomicLong) DecrementAndGet(ctx context.Context) (int64, error) {
	return a.AddAndGet(ctx, -1)
}

// GetAndDecrement atomically decrements the current value by one.
func (a AtomicLong) GetAndDecrement(ctx context.Context) (int64, error) {
	return a.GetAndAdd(ctx, -1)
}

// GetAndIncrement atomically increments the current value by one.
func (a AtomicLong) GetAndIncrement(ctx context.Context) (int64, error) {
	return a.GetAndAdd(ctx, 1)
}
