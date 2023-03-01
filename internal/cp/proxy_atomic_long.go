/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cp

import (
	"context"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

const (
	alterValueTypeOldValue = 0
	alterValueTypeNewValue = 1
)

/*
AtomicLong is a redundant and highly available distributed counter for 64-bit integers (“long“ type in Java).
It works on top of the Raft consensus algorithm.
It offers linearizability during crash failures and network partitions.
It is CP with respect to the CAP principle.
If a network partition occurs, it remains available on at most one side of the partition.
AtomicLong implementation does not offer exactly-once / effectively-once execution semantics.
It goes with at-least-once execution semantics by default and can cause an API call to be committed multiple times in case of CP member failures.
It can be tuned to offer at-most-once execution semantics.
See fail-on-indeterminate-operation-state server-side setting: https://docs.hazelcast.com/hazelcast/5.2/cp-subsystem/configuration#global-configuration-options
*/
type AtomicLong struct {
	*proxy
}

/*
AtomicLong implementation is type aliased in the public API so all the exported fields and methods are directly accessible by users.
Be aware of that while editing the fields and methods of both proxy and AtomicLong structs.
*/

// AddAndGet atomically adds the given value to the current value.
func (a *AtomicLong) AddAndGet(ctx context.Context, delta int64) (int64, error) {
	request := codec.EncodeAtomicLongAddAndGetRequest(a.groupID, a.name, delta)
	response, err := a.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return 0, err
	}
	return codec.DecodeAtomicLongAddAndGetResponse(response), nil
}

// CompareAndSet Atomically sets the value to the given updated value only if the current value equals the expected value.
func (a *AtomicLong) CompareAndSet(ctx context.Context, expect int64, update int64) (bool, error) {
	request := codec.EncodeAtomicLongCompareAndSetRequest(a.groupID, a.name, expect, update)
	response, err := a.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return false, err
	}
	return codec.DecodeAtomicLongCompareAndSetResponse(response), nil
}

// Get gets the current value.
func (a *AtomicLong) Get(ctx context.Context) (int64, error) {
	request := codec.EncodeAtomicLongGetRequest(a.groupID, a.name)
	response, err := a.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return 0, err
	}
	return codec.DecodeAtomicLongGetResponse(response), nil

}

// GetAndAdd atomically adds the given value to the current value.
func (a *AtomicLong) GetAndAdd(ctx context.Context, delta int64) (int64, error) {
	request := codec.EncodeAtomicLongGetAndAddRequest(a.groupID, a.name, delta)
	response, err := a.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return 0, err
	}
	return codec.DecodeAtomicLongGetAndAddResponse(response), nil

}

// GetAndSet atomically sets the given value and returns the old value.
func (a *AtomicLong) GetAndSet(ctx context.Context, value int64) (int64, error) {
	request := codec.EncodeAtomicLongGetAndSetRequest(a.groupID, a.name, value)
	response, err := a.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return 0, err
	}
	return codec.DecodeAtomicLongGetAndSetResponse(response), nil
}

// Set atomically sets the given value.
func (a *AtomicLong) Set(ctx context.Context, value int64) error {
	request := codec.EncodeAtomicLongGetAndSetRequest(a.groupID, a.name, value)
	_, err := a.invokeOnRandomTarget(ctx, request, nil)
	return err
}

// Apply applies a function on the value, the actual stored value will not change.
// function must be an instance of Hazelcast serializable type.
// It must have a counterpart registered in the server-side that implements the "com.hazelcast.core.IFunction" interface with the actual logic of the function to be applied.
func (a *AtomicLong) Apply(ctx context.Context, function interface{}) (interface{}, error) {
	data, err := a.ss.ToData(function)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeAtomicLongApplyRequest(a.groupID, a.object, data)
	response, err := a.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	obj, err := a.ss.ToObject(codec.DecodeAtomicLongApplyResponse(response))
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (a *AtomicLong) alter(ctx context.Context, function interface{}, valueType int32) (*proto.ClientMessage, error) {
	data, err := a.ss.ToData(function)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeAtomicLongAlterRequest(a.groupID, a.object, data, valueType)
	response, err := a.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (a *AtomicLong) alterAndReturn(ctx context.Context, function interface{}, alterValue int32) (int64, error) {
	response, err := a.alter(ctx, function, alterValue)
	if err != nil {
		return 0, err
	}
	return codec.DecodeAtomicLongAlterResponse(response), nil
}

// Alter alters the currently stored value by applying a function on it.
// function must be an instance of Hazelcast serializable type.
// It must have a counterpart registered in the server-side that implements the "com.hazelcast.core.IFunction" interface with the actual logic of the function to be applied.
func (a *AtomicLong) Alter(ctx context.Context, function interface{}) error {
	_, err := a.alter(ctx, function, alterValueTypeNewValue)
	return err
}

// GetAndAlter alters the currently stored value by applying a function on it and gets the old value.
// function must be an instance of Hazelcast serializable type.
// It must have a counterpart registered in the server-side that implements the "com.hazelcast.core.IFunction" interface with the actual logic of the function to be applied.
func (a *AtomicLong) GetAndAlter(ctx context.Context, function interface{}) (int64, error) {
	return a.alterAndReturn(ctx, function, alterValueTypeOldValue)
}

// AlterAndGet alters the currently stored value by applying a function on it and gets the result.
// function must be an instance of Hazelcast serializable type.
// It must have a counterpart registered in the server-side that implements the "com.hazelcast.core.IFunction" interface with the actual logic of the function to be applied.
func (a *AtomicLong) AlterAndGet(ctx context.Context, function interface{}) (int64, error) {
	return a.alterAndReturn(ctx, function, alterValueTypeNewValue)
}

// IncrementAndGet atomically increments the current value by one.
func (a *AtomicLong) IncrementAndGet(ctx context.Context) (int64, error) {
	return a.AddAndGet(ctx, 1)
}

// DecrementAndGet atomically decrements the current value by one.
func (a *AtomicLong) DecrementAndGet(ctx context.Context) (int64, error) {
	return a.AddAndGet(ctx, -1)
}

// GetAndDecrement atomically decrements the current value by one.
func (a *AtomicLong) GetAndDecrement(ctx context.Context) (int64, error) {
	return a.GetAndAdd(ctx, -1)
}

// GetAndIncrement atomically increments the current value by one.
func (a *AtomicLong) GetAndIncrement(ctx context.Context) (int64, error) {
	return a.GetAndAdd(ctx, 1)
}
