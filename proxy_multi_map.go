/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package hazelcast

import (
	"context"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

/*
MultiMap is a distributed map.
Hazelcast Go client enables you to perform operations like reading and writing from/to a Hazelcast MultiMap with methods like Get and Put.
For details, see https://docs.hazelcast.com/hazelcast/latest/data-structures/multimap.html

Listening for MultiMap Entry Events

The first step of listening to entry-based events is  creating an instance of MultiMapEntryListenerConfig.
MultiMapEntryListenerConfig contains options to filter the events by key and has an option to include the value of the entry, not just the key.
You should also choose which type of events you want to receive.
In the example below, a listener configuration for added and updated entries is created.
Entries only with key "somekey":

	entryListenerConfig := hazelcast.MultiMapEntryListenerConfig{
		Key: "somekey",
		IncludeValue: true,
	}
	entryListenerConfig.NotifyEntryAdded(true)
	entryListenerConfig.NotifyEntryUpdated(true)
	m, err := client.GetMap(ctx, "somemap")

After creating the configuration, the second step is adding an event listener and a handler to act on received events:

	subscriptionID, err := m.AddEntryListener(ctx, entryListenerConfig, func(event *hazelcast.EntryNotified) {
		switch event.EventType {
		case hazelcast.EntryAdded:
			fmt.Println("Entry Added:", event.Value)
		case hazelcast.EntryRemoved:
			fmt.Println("Entry Removed:", event.Value)
		case hazelcast.EntryUpdated:
			fmt.Println("Entry Updated:", event.Value)
		case hazelcast.EntryEvicted:
			fmt.Println("Entry Remove:", event.Value)
		case hazelcast.EntryLoaded:
			fmt.Println("Entry Loaded:", event.Value)
		}
	})

Adding an event listener returns a subscription ID, which you can later use to remove the listener:

	err = m.RemoveEntryListener(ctx, subscriptionID)

Using Locks

You can lock entries in a MultiMap.
When an entry is locked, only the owner of that lock can access that entry in the cluster until it is unlocked by the owner of force unlocked.
See https://docs.hazelcast.com/imdg/latest/data-structures/map.html#locking-maps for details, usage is identical.

Locks are reentrant.
The owner of a lock can acquire the lock again without waiting for the lock to be unlocked.
If the key is locked N times, it should be unlocked N times before another goroutine can acquire it.

Lock ownership in Hazelcast Go Client is explicit.
The first step to own a lock is creating a lock context, which is similar to a key.
The lock context is a regular context.Context which carry a special value that uniquely identifies the lock context in the cluster.
Once the lock context is created, it can be used to lock/unlock entries and used with any function that is lock aware, such as Put.

	m, err := client.GetMultiMap(ctx, "my-map")
	lockCtx := m.NewLockContext(ctx)
	// block acquiring the lock
	err = m.Lock(lockCtx, "some-key")
	// pass lock context to use the locked entry
	err = m.Put(lockCtx, "some-key", "some-value")
	// release the lock once done with it
	err = m.Unlock(lockCtx, "some-key")

As mentioned before, lock context is a regular context.Context which carry a special lock ID.
You can pass any context.Context to any MultiMap function, but in that case lock ownership between operations using the same hazelcast.Client instance is not possible.

*/
type MultiMap struct {
	*proxy
}

func newMultiMap(p *proxy) *MultiMap {
	return &MultiMap{proxy: p}
}

// NewLockContext augments the passed parent context with a unique lock ID.
// If passed context is nil, context.Background is used as the parent context.
func (m *MultiMap) NewLockContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, lockIDKey{}, lockID(m.refIDGen.NextID()))
}

// AddEntryListener adds a continuous entry listener to this multi-map.
func (m *MultiMap) AddEntryListener(ctx context.Context, config MultiMapEntryListenerConfig, handler EntryNotifiedHandler) (types.UUID, error) {
	return m.addEntryListener(ctx, config.IncludeValue, m.smart, config.Key, func(event *EntryNotified) {
		if int32(event.EventType)&config.flags == 0 {
			return
		}
		handler(event)
	})
}

// Clear deletes all entries one by one and fires related events.
func (m *MultiMap) Clear(ctx context.Context) error {
	request := codec.EncodeMultiMapClearRequest(m.name)
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

// ContainsKey returns true if the map contains an entry with the given key.
func (m *MultiMap) ContainsKey(ctx context.Context, key interface{}) (bool, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeMultiMapContainsKeyRequest(m.name, keyData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMultiMapContainsKeyResponse(response), nil
		}
	}
}

// ContainsValue returns true if the map contains an entry with the given value.
func (m *MultiMap) ContainsValue(ctx context.Context, value interface{}) (bool, error) {
	if valueData, err := m.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeMultiMapContainsValueRequest(m.name, valueData)
		if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
			return false, err
		} else {
			return codec.DecodeMultiMapContainsValueResponse(response), nil
		}
	}
}

// ContainsEntry returns true if the multi-map contains an entry with the given key and value.
func (m *MultiMap) ContainsEntry(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	lid := extractLockID(ctx)
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	valueData, err := m.validateAndSerialize(value)
	if err != nil {
		return false, err
	}
	request := codec.EncodeMultiMapContainsEntryRequest(m.name, keyData, valueData, lid)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return false, err
	}
	return codec.DecodeMultiMapContainsEntryResponse(response), nil
}

// Delete removes the mapping for a key from this multi-map if it is present.
// Unlike remove(object), this operation does not return the removed value, which avoids the serialization cost of
// the returned value. If the removed value will not be used, delete operation is preferred over remove
// operation for better performance.
func (m *MultiMap) Delete(ctx context.Context, key interface{}) error {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		request := codec.EncodeMultiMapDeleteRequest(m.name, keyData, lid)
		_, err := m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

// ForceUnlock releases the lock for the specified key regardless of the lock owner.
// It always successfully unlocks the key, never blocks, and returns immediately.
func (m *MultiMap) ForceUnlock(ctx context.Context, key interface{}) error {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.refIDGen.NextID()
		request := codec.EncodeMultiMapForceUnlockRequest(m.name, keyData, refID)
		_, err = m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

// Get returns the value for the specified key, or nil if this multi-map does not contain this key.
// Warning:
// This method returns a clone of original value, modifying the returned value does not change the
// actual value in the multi-map. One should put modified value back to make changes visible to all nodes.
func (m *MultiMap) Get(ctx context.Context, key interface{}) ([]interface{}, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMultiMapGetRequest(m.name, keyData, lid)

		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObjects(codec.DecodeMultiMapGetResponse(response))
		}
	}
}

// GetEntrySet returns a clone of the mappings contained in this multi-map.
func (m *MultiMap) GetEntrySet(ctx context.Context) ([]types.Entry, error) {
	request := codec.EncodeMultiMapEntrySetRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return nil, err
	} else {
		return m.convertPairsToEntries(codec.DecodeMultiMapEntrySetResponse(response))
	}
}

// GetKeySet returns keys contained in this map.
func (m *MultiMap) GetKeySet(ctx context.Context) ([]interface{}, error) {
	request := codec.EncodeMultiMapKeySetRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return nil, err
	} else {
		keyDatas := codec.DecodeMultiMapKeySetResponse(response)
		keys := make([]interface{}, len(keyDatas))
		for i, keyData := range keyDatas {
			if key, err := m.convertToObject(keyData); err != nil {
				return nil, err
			} else {
				keys[i] = key
			}
		}
		return keys, nil
	}
}

// GetValues returns a list clone of the values contained in this map.
func (m *MultiMap) GetValues(ctx context.Context) ([]interface{}, error) {
	request := codec.EncodeMultiMapValuesRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return nil, err
	} else {
		return m.convertToObjects(codec.DecodeMultiMapValuesResponse(response))
	}
}

// IsLocked checks the lock for the specified key.
func (m *MultiMap) IsLocked(ctx context.Context, key interface{}) (bool, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeMultiMapIsLockedRequest(m.name, keyData)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMultiMapIsLockedResponse(response), nil
		}
	}
}

/*
Lock acquires the lock for the specified key infinitely.
If the lock is not available, the current goroutine is blocked until the lock is acquired using the same lock context.

You get a lock whether the value is present in the multi-map or not.
Other goroutines or threads on other systems would block on their invoke of Lock until the non-existent key is unlocked.
If the lock holder introduces the key to the map, the Put operation is not blocked.
If a goroutine not holding a lock on the non-existent key tries to introduce the key while a lock exists on the non-existent key, the Put operation blocks until it is unlocked.

Scope of the lock is this MultiMap only.
Acquired lock is only for the key in this map.

Locks are re-entrant.
If the key is locked N times, it should be unlocked N times before another goroutine can acquire it.
*/
func (m *MultiMap) Lock(ctx context.Context, key interface{}) error {
	return m.lock(ctx, key, ttlUnset)
}

// LockWithLease acquires the lock for the specified lease time.
// Otherwise, it behaves the same as Lock function.
func (m *MultiMap) LockWithLease(ctx context.Context, key interface{}, leaseTime time.Duration) error {
	return m.lock(ctx, key, leaseTime.Milliseconds())
}

// Put appends the value for the given key to the corresponding value list and returns if operation is successful.
func (m *MultiMap) Put(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	return m.put(ctx, key, value)
}

// PutAll appends given values to the value list of given key.
// No atomicity guarantees are given. In the case of a failure, some key-value tuples may get written,
// while others are not.
func (m *MultiMap) PutAll(ctx context.Context, key interface{}, values ...interface{}) error {
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return err
	}
	valuesData, err := m.validateAndSerializeValues(values)
	if err != nil {
		return err
	}
	request := codec.EncodeMultiMapPutAllRequest(m.name, []proto.Pair{proto.NewPair(keyData, valuesData)})
	if _, err := m.invokeOnKey(ctx, request, keyData); err != nil {
		return err
	}
	return nil
}

// Remove deletes all the values corresponding to the given key and returns them as a slice.
func (m *MultiMap) Remove(ctx context.Context, key interface{}) ([]interface{}, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMultiMapRemoveRequest(m.name, keyData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObjects(codec.DecodeMultiMapRemoveResponse(response))
		}
	}
}

// RemoveEntry removes the specified value for the given key and returns true if call had an effect.
func (m *MultiMap) RemoveEntry(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	lid := extractLockID(ctx)
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	valueData, err := m.validateAndSerialize(value)
	if err != nil {
		return false, err
	}
	request := codec.EncodeMultiMapRemoveEntryRequest(m.name, keyData, valueData, lid)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return false, err
	}
	return codec.DecodeMultiMapRemoveEntryResponse(response), nil
}

// RemoveEntryListener removes the specified entry listener.
func (m *MultiMap) RemoveEntryListener(ctx context.Context, subscriptionID types.UUID) error {
	return m.listenerBinder.Remove(ctx, subscriptionID)
}

// Size returns the number of entries in this multi-map.
func (m *MultiMap) Size(ctx context.Context) (int, error) {
	request := codec.EncodeMultiMapSizeRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeMultiMapSizeResponse(response)), nil
	}
}

// TryLock tries to acquire the lock for the specified key.
// When the lock is not available, the current goroutine doesn't wait and returns false immediately.
func (m *MultiMap) TryLock(ctx context.Context, key interface{}) (bool, error) {
	return m.tryLock(ctx, key, leaseUnset, 0)
}

// TryLockWithLease tries to acquire the lock for the specified key.
// Lock will be released after lease time passes.
func (m *MultiMap) TryLockWithLease(ctx context.Context, key interface{}, lease time.Duration) (bool, error) {
	return m.tryLock(ctx, key, lease.Milliseconds(), 0)
}

// TryLockWithTimeout tries to acquire the lock for the specified key.
// The current goroutine is blocked until the lock is acquired using the same lock context, or he specified waiting time elapses.
func (m *MultiMap) TryLockWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (bool, error) {
	return m.tryLock(ctx, key, leaseUnset, timeout.Milliseconds())
}

// TryLockWithLeaseAndTimeout tries to acquire the lock for the specified key.
// The current goroutine is blocked until the lock is acquired using the same lock context, or he specified waiting time elapses.
// Lock will be released after lease time passes.
func (m *MultiMap) TryLockWithLeaseAndTimeout(ctx context.Context, key interface{}, lease time.Duration, timeout time.Duration) (bool, error) {
	return m.tryLock(ctx, key, lease.Milliseconds(), timeout.Milliseconds())
}

// Unlock releases the lock for the specified key.
func (m *MultiMap) Unlock(ctx context.Context, key interface{}) error {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.refIDGen.NextID()
		request := codec.EncodeMultiMapUnlockRequest(m.name, keyData, lid, refID)
		_, err = m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

func (m *MultiMap) addEntryListener(ctx context.Context, includeValue, localOnly bool, key interface{}, handler EntryNotifiedHandler) (types.UUID, error) {
	var err error
	var keyData serialization.Data
	if key != nil {
		if keyData, err = m.validateAndSerialize(key); err != nil {
			return types.UUID{}, err
		}
	}
	subscriptionID := types.NewUUID()
	addRequest := m.makeListenerRequest(keyData, includeValue, localOnly)
	listenerHandler := func(msg *proto.ClientMessage) {
		m.makeListenerDecoder(msg, keyData, m.makeEntryNotifiedListenerHandler(handler))
	}
	removeRequest := codec.EncodeMultiMapRemoveEntryListenerRequest(m.name, subscriptionID)
	err = m.listenerBinder.Add(ctx, subscriptionID, addRequest, removeRequest, listenerHandler)
	return subscriptionID, err
}

func (m *MultiMap) lock(ctx context.Context, key interface{}, ttl int64) error {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.refIDGen.NextID()
		request := codec.EncodeMultiMapLockRequest(m.name, keyData, lid, ttl, refID)
		_, err = m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

func (m *MultiMap) put(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	lid := extractLockID(ctx)
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return false, err
	} else {
		request := codec.EncodeMultiMapPutRequest(m.name, keyData, valueData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMultiMapPutResponse(response), nil
		}
	}
}

func (m *MultiMap) tryLock(ctx context.Context, key interface{}, lease int64, timeout int64) (bool, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		refID := m.refIDGen.NextID()
		request := codec.EncodeMultiMapTryLockRequest(m.name, keyData, lid, lease, timeout, refID)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMultiMapTryLockResponse(response), nil
		}
	}
}

func (m *MultiMap) makeListenerRequest(keyData serialization.Data, includeValue, localOnly bool) *proto.ClientMessage {
	if keyData.Payload != nil {
		return codec.EncodeMultiMapAddEntryListenerToKeyRequest(m.name, keyData, includeValue, localOnly)
	}
	return codec.EncodeMultiMapAddEntryListenerRequest(m.name, includeValue, localOnly)
}

func (m *MultiMap) makeListenerDecoder(msg *proto.ClientMessage, keyData serialization.Data, handler entryNotifiedHandler) {
	if keyData.Payload != nil {
		codec.HandleMultiMapAddEntryListenerToKey(msg, handler)
		return
	}
	codec.HandleMultiMapAddEntryListener(msg, handler)
}

// MultiMapEntryListenerConfig contains configuration for a multi-map entry listener.
type MultiMapEntryListenerConfig struct {
	Key          interface{}
	flags        int32
	IncludeValue bool
}

// NotifyEntryAdded enables receiving an entry event when an entry is added.
func (c *MultiMapEntryListenerConfig) NotifyEntryAdded(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryAdded), enable)
}

// NotifyEntryRemoved enables receiving an entry event when an entry is removed.
func (c *MultiMapEntryListenerConfig) NotifyEntryRemoved(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryRemoved), enable)
}

// NotifyEntryAllCleared enables receiving an entry event when all entries are cleared.
func (c *MultiMapEntryListenerConfig) NotifyEntryAllCleared(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryAllCleared), enable)
}
