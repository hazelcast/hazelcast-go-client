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

	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

/*
AtomicRef is a distributed, highly available object reference with atomic operations.

AtomicReference offers linearizability during crash failures and network partitions.
It is CP with respect to the CAP principle.
If a network partition occurs, it remains available on at most one side of the partition.

The following are some considerations you need to know when you use AtomicReference:

  - AtomicReference works based on the byte-content and not on the object-reference.
    If you use the CompareAndSet method, do not change the original value because its serialized content will then be different.
  - All methods returning an object return a private copy.
    You can modify the private copy, but the rest of the world is shielded from your changes.
    If you want these changes to be visible to the rest of the world, you need to write the change back to the AtomicReference; but be careful not to introduce a data-race.
  - The in-memory format of an AtomicReference is “binary“.
    The receiving side does not need to have the class definition available unless it needs to be deserialized on the other side.
    This deserialization is done for every call that needs to have the object instead of the binary content, so be careful with expensive object graphs that need to be deserialized.

IAtomicReference does not offer exactly-once / effectively-once execution semantics.
It goes with at-least-once execution semantics by default and can cause an API call to be committed multiple times in case of CP member failures.
It can be tuned to offer at-most-once execution semantics.
See the `fail-on-indeterminate-operation-state` server-side setting.
*/
type AtomicRef struct {
	*proxy
}

// Clear sets the value to nil.
func (ar *AtomicRef) Clear(ctx context.Context) error {
	return ar.Set(ctx, nil)
}

// Contains returns true if the saved value is the given value.
func (ar *AtomicRef) Contains(ctx context.Context, value any) (bool, error) {
	vd, err := ar.ss.ToData(value)
	if err != nil {
		return false, err
	}
	req := codec.EncodeAtomicRefContainsRequest(ar.groupID, ar.name, vd)
	resp, err := ar.invokeOnRandomTarget(ctx, req, nil)
	if err != nil {
		return false, err
	}
	return codec.DecodeAtomicRefContainsResponse(resp), nil
}

// CompareAndSet atomically sets the value to the given updated value only if the given value is equal to the expected value.
// Returns true if the value was set.
func (ar *AtomicRef) CompareAndSet(ctx context.Context, expectedValue, newValue any) (bool, error) {
	ed, err := ar.ss.ToData(expectedValue)
	if err != nil {
		return false, err
	}
	nd, err := ar.ss.ToData(newValue)
	if err != nil {
		return false, err
	}
	req := codec.EncodeAtomicRefCompareAndSetRequest(ar.groupID, ar.name, ed, nd)
	resp, err := ar.invokeOnRandomTarget(ctx, req, nil)
	if err != nil {
		return false, err
	}
	return codec.DecodeAtomicRefCompareAndSetResponse(resp), nil
}

// Get returns the current value.
func (ar *AtomicRef) Get(ctx context.Context) (any, error) {
	data, err := AtomicRefGetData(ctx, ar)
	if err != nil {
		return nil, err
	}
	return ar.ss.ToObject(data)
}

// GetAndSet gets the old value and sets the new value.
func (ar *AtomicRef) GetAndSet(ctx context.Context, value any) (any, error) {
	vd, err := ar.ss.ToData(value)
	if err != nil {
		return nil, err
	}
	data, err := AtomicRefGetAndSetData(ctx, ar, vd)
	if err != nil {
		return nil, err
	}
	return ar.ss.ToObject(data)
}

// IsNil returns true if the current value is nil.
func (ar *AtomicRef) IsNil(ctx context.Context) (bool, error) {
	return ar.Contains(ctx, nil)
}

// Set atomically sets the given value.
func (ar *AtomicRef) Set(ctx context.Context, value any) error {
	data, err := ar.ss.ToData(value)
	if err != nil {
		return err
	}
	req := codec.EncodeAtomicRefSetRequest(ar.groupID, ar.name, data, false)
	if _, err := ar.invokeOnRandomTarget(ctx, req, nil); err != nil {
		return err
	}
	return nil
}

// AtomicRefGetData returns the current value as serialization.Data.
// This is an internal function exposed only in the Go Extensibility API.
func AtomicRefGetData(ctx context.Context, ar *AtomicRef) (iserialization.Data, error) {
	req := codec.EncodeAtomicRefGetRequest(ar.groupID, ar.name)
	resp, err := ar.invokeOnRandomTarget(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	return codec.DecodeAtomicRefGetResponse(resp), nil
}

// AtomicRefGetAndSetData sets the new value and returns the old value as serialization.Data.
// This is an internal function exposed only in the Go Extensibility API.
func AtomicRefGetAndSetData(ctx context.Context, ar *AtomicRef, valueData iserialization.Data) (iserialization.Data, error) {
	req := codec.EncodeAtomicRefSetRequest(ar.groupID, ar.name, valueData, true)
	resp, err := ar.invokeOnRandomTarget(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	return codec.DecodeAtomicRefSetResponse(resp), nil
}
