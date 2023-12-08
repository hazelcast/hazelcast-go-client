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

	"github.com/hazelcast/hazelcast-go-client/internal/check"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type Map struct {
	*proxy
}

// Set sets the given value to the given key.
// Both key and value must be non-nil.
func (m *Map) Set(ctx context.Context, key, value any) error {
	kd, err := m.validateAndSerialize(key)
	if err != nil {
		return err
	}
	vd, err := m.validateAndSerialize(value)
	if err != nil {
		return err
	}
	req := codec.EncodeCPMapSetRequest(m.groupID, m.name, kd, vd)
	if _, err := m.invokeOnRandomTarget(ctx, req, nil); err != nil {
		return err
	}
	return nil
}

// Get returns the value for the given key.
// key must be non-nil.
func (m *Map) Get(ctx context.Context, key any) (any, error) {
	kd, err := m.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	vd, err := CPMapGetData(ctx, m, kd)
	return m.ss.ToObject(vd)
}

// Put sets the given value to the given key and returns the old value.
// Both key and value must be non-nil.
func (m *Map) Put(ctx context.Context, key, value any) (any, error) {
	kd, err := m.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	vd, err := m.validateAndSerialize(value)
	if err != nil {
		return nil, err
	}
	data, err := CPMapPutData(ctx, m, kd, vd)
	if err != nil {
		return nil, err
	}
	return m.ss.ToObject(data)
}

// Remove deletes the value for the given key and returns the value.
// key must be non-nil.
func (m *Map) Remove(ctx context.Context, key any) (any, error) {
	kd, err := m.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	vd, err := CPMapRemoveData(ctx, m, kd)
	return m.ss.ToObject(vd)
}

// Delete deletes the value for the given key.
// key must be non-nil.
func (m *Map) Delete(ctx context.Context, key any) error {
	kd, err := m.validateAndSerialize(key)
	if err != nil {
		return err
	}
	req := codec.EncodeCPMapDeleteRequest(m.groupID, m.name, kd)
	if _, err := m.invokeOnRandomTarget(ctx, req, nil); err != nil {
		return err
	}
	return nil
}

// CompareAndSet atomically sets key to newValue if the current value for key is equal-to expectedValue.
// Returns true if the value for key is changed.
// All of key, expectedValue and newValue must be non-nil.
func (m *Map) CompareAndSet(ctx context.Context, key, expectedValue, newValue any) (bool, error) {
	kd, err := m.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	ed, err := m.validateAndSerialize(expectedValue)
	if err != nil {
		return false, err
	}
	nd, err := m.validateAndSerialize(newValue)
	if err != nil {
		return false, err
	}
	req := codec.EncodeCPMapCompareAndSetRequest(m.groupID, m.name, kd, ed, nd)
	resp, err := m.invokeOnRandomTarget(ctx, req, nil)
	ok := codec.DecodeCPMapCompareAndSetResponse(resp)
	return ok, nil
}

func (m *Map) validateAndSerialize(arg1 interface{}) (iserialization.Data, error) {
	if check.Nil(arg1) {
		return nil, ihzerrors.NewIllegalArgumentError("nil arg is not allowed", nil)
	}
	return m.ss.ToData(arg1)
}

func CPMapGetData(ctx context.Context, m *Map, keyData iserialization.Data) (iserialization.Data, error) {
	req := codec.EncodeCPMapGetRequest(m.groupID, m.name, keyData)
	resp, err := m.invokeOnRandomTarget(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	vd := codec.DecodeCPMapGetResponse(resp)
	return vd, nil
}

func CPMapPutData(ctx context.Context, m *Map, keyData, valueData iserialization.Data) (iserialization.Data, error) {
	req := codec.EncodeCPMapPutRequest(m.groupID, m.name, keyData, valueData)
	resp, err := m.invokeOnRandomTarget(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	vd := codec.DecodeCPMapPutResponse(resp)
	return vd, nil
}

func CPMapRemoveData(ctx context.Context, m *Map, keyData iserialization.Data) (iserialization.Data, error) {
	req := codec.EncodeCPMapRemoveRequest(m.groupID, m.name, keyData)
	resp, err := m.invokeOnRandomTarget(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	vd := codec.DecodeCPMapRemoveResponse(resp)
	return vd, nil
}
