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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	maxIndexAttributes = 255
	defaultLockID      = 0
	lockIDKey          = "__hz_lock_id"
)

type lockID int64

type Map struct {
	cm *ContextMap
}

func newMap(p *proxy, refIDGenerator *iproxy.ReferenceIDGenerator) *Map {
	m := newContextMap(p, refIDGenerator)
	return &Map{
		cm: m,
	}
}

func (m *Map) ContextAware() *ContextMap {
	return m.cm
}

// AddEntryListener adds a continuous entry listener to this map.
func (m *Map) AddEntryListener(config MapEntryListenerConfig, handler EntryNotifiedHandler) (types.UUID, error) {
	return m.cm.AddEntryListener(context.Background(), config, handler)
}

// AddIndexWithConfig adds an index to this map for the specified entries so that queries can run faster.
func (m *Map) AddIndexWithConfig(indexConfig types.IndexConfig) error {
	return m.cm.addIndex(context.Background(), indexConfig)
}

// AddInterceptor adds an interceptor for this map.
func (m *Map) AddInterceptor(interceptor interface{}) (string, error) {
	return m.cm.AddInterceptor(context.Background(), interceptor)
}

// Clear deletes all entries one by one and fires related events
func (m *Map) Clear() error {
	return m.cm.Clear(context.Background())
}

// ContainsKey returns true if the map contains an entry with the given key
func (m *Map) ContainsKey(key interface{}) (bool, error) {
	return m.cm.ContainsKey(context.Background(), key)
}

// ContainsValue returns true if the map contains an entry with the given value
func (m *Map) ContainsValue(value interface{}) (bool, error) {
	return m.cm.ContainsValue(context.Background(), value)
}

// Delete removes the mapping for a key from this map if it is present
// Unlike remove(object), this operation does not return the removed value, which avoids the serialization cost of
// the returned value. If the removed value will not be used, a delete operation is preferred over a remove
// operation for better performance.
func (m *Map) Delete(key interface{}) error {
	return m.cm.Delete(context.Background(), key)
}

// Destroys this object cluster-wide.
// Clears and releases all resources for this object.
func (m *Map) Destroy() error {
	return m.cm.Destroy()
}

// Evict evicts the mapping for a key from this map.
// Returns true if the key is evicted.
func (m *Map) Evict(key interface{}) (bool, error) {
	return m.cm.Evict(context.Background(), key)
}

// EvictAll deletes all entries withour firing releated events
func (m *Map) EvictAll() error {
	return m.cm.EvictAll(context.Background())
}

// ExecuteOnEntries applies the user defined EntryProcessor to all the entries in the map.
func (m *Map) ExecuteOnEntries(entryProcessor interface{}) ([]types.Entry, error) {
	return m.cm.ExecuteOnEntries(context.Background(), entryProcessor)
}

// Flush flushes all the local dirty entries.
func (m *Map) Flush() error {
	return m.cm.Flush(context.Background())
}

// Get returns the value for the specified key, or nil if this map does not contain this key.
// Warning:
// This method returns a clone of original value, modifying the returned value does not change the
// actual value in the map. One should put modified value back to make changes visible to all nodes.
func (m *Map) Get(key interface{}) (interface{}, error) {
	return m.cm.Get(context.Background(), key)
}

// GetAll returns the entries for the given keys.
func (m *Map) GetAll(keys ...interface{}) ([]types.Entry, error) {
	return m.cm.GetAll(context.Background(), keys...)
}

// GetEntrySet returns a clone of the mappings contained in this map.
func (m *Map) GetEntrySet() ([]types.Entry, error) {
	return m.cm.GetEntrySet(context.Background())
}

// GetEntrySetWithPredicate returns a clone of the mappings contained in this map.
func (m *Map) GetEntrySetWithPredicate(predicate predicate.Predicate) ([]types.Entry, error) {
	return m.cm.GetEntrySetWithPredicate(context.Background(), predicate)
}

// GetEntryView returns the SimpleEntryView for the specified key.
func (m *Map) GetEntryView(key string) (*types.SimpleEntryView, error) {
	return m.cm.GetEntryView(context.Background(), key)
}

// GetKeySet returns keys contained in this map
func (m *Map) GetKeySet() ([]interface{}, error) {
	return m.cm.GetKeySet(context.Background())
}

// GetKeySetWithPredicate returns keys contained in this map
func (m *Map) GetKeySetWithPredicate(predicate predicate.Predicate) ([]interface{}, error) {
	return m.cm.GetKeySetWithPredicate(context.Background(), predicate)
}

// GetValues returns a list clone of the values contained in this map
func (m *Map) GetValues() ([]interface{}, error) {
	return m.cm.GetValues(context.Background())
}

// GetValuesWithPredicate returns a list clone of the values contained in this map
func (m *Map) GetValuesWithPredicate(predicate predicate.Predicate) ([]interface{}, error) {
	return m.cm.GetValuesWithPredicate(context.Background(), predicate)
}

// IsEmpty returns true if this map contains no key-value mappings.
func (m *Map) IsEmpty() (bool, error) {
	return m.cm.IsEmpty(context.Background())
}

// LoadAllWithoutReplacing loads all keys from the store at server side or loads the given keys if provided.
func (m *Map) LoadAllWithoutReplacing(keys ...interface{}) error {
	return m.cm.LoadAllWithoutReplacing(context.Background(), keys...)
}

// LoadAllReplacing loads all keys from the store at server side or loads the given keys if provided.
// Replaces existing keys.
func (m *Map) LoadAllReplacing(keys ...interface{}) error {
	return m.cm.LoadAllReplacing(context.Background(), keys...)
}

// Put sets the value for the given key and returns the old value.
func (m *Map) Put(key interface{}, value interface{}) (interface{}, error) {
	return m.cm.Put(context.Background(), key, value)
}

// PutWithTTL sets the value for the given key and returns the old value.
// Entry will expire and get evicted after the ttl.
func (m *Map) PutWithTTL(key interface{}, value interface{}, ttl time.Duration) (interface{}, error) {
	return m.cm.PutWithTTL(context.Background(), key, value, ttl)
}

// PutWithMaxIdle sets the value for the given key and returns the old value.
// maxIdle is the maximum time in seconds for this entry to stay idle in the map.
func (m *Map) PutWithMaxIdle(key interface{}, value interface{}, maxIdle time.Duration) (interface{}, error) {
	return m.cm.PutWithMaxIdle(context.Background(), key, value, maxIdle)
}

// PutWithTTLAndMaxIdle sets the value for the given key and returns the old value.
// Entry will expire and get evicted after the ttl.
// maxIdle is the maximum time in seconds for this entry to stay idle in the map.
func (m *Map) PutWithTTLAndMaxIdle(key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) (interface{}, error) {
	return m.cm.PutWithTTLAndMaxIdle(context.Background(), key, value, ttl, maxIdle)
}

// PutAll copies all of the mappings from the specified map to this map.
// No atomicity guarantees are given. In the case of a failure, some of the key-value tuples may get written,
// while others are not.
func (m *Map) PutAll(keyValuePairs ...types.Entry) error {
	return m.cm.PutAll(context.Background(), keyValuePairs...)
}

// PutIfAbsent associates the specified key with the given value if it is not already associated.
func (m *Map) PutIfAbsent(key interface{}, value interface{}) (interface{}, error) {
	return m.cm.PutIfAbsent(context.Background(), key, value)
}

// PutIfAbsentWithTTL associates the specified key with the given value if it is not already associated.
// Entry will expire and get evicted after the ttl.
func (m *Map) PutIfAbsentWithTTL(key interface{}, value interface{}, ttl time.Duration) (interface{}, error) {
	return m.cm.PutIfAbsentWithTTL(context.Background(), key, value, ttl)
}

// PutIfAbsentWithTTLAndMaxIdle associates the specified key with the given value if it is not already associated.
// Entry will expire and get evicted after the ttl.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
func (m *Map) PutIfAbsentWithTTLAndMaxIdle(key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) (interface{}, error) {
	return m.cm.PutIfAbsentWithTTLAndMaxIdle(context.Background(), key, value, ttl, maxIdle)
}

// PutTransient sets the value for the given key.
// MapStore defined at the server side will not be called.
// The TTL defined on the server-side configuration will be used.
// Max idle time defined on the server-side configuration will be used.
func (m *Map) PutTransient(key interface{}, value interface{}) error {
	return m.cm.PutTransient(context.Background(), key, value)
}

// PutTransientWithTTL sets the value for the given key.
// MapStore defined at the server side will not be called.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
func (m *Map) PutTransientWithTTL(key interface{}, value interface{}, ttl time.Duration) error {
	return m.cm.PutTransientWithTTL(context.Background(), key, value, ttl)
}

// PutTransientWithMaxIdle sets the value for the given key.
// MapStore defined at the server side will not be called.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
// Set maxIdle to 0 for infinite idle time.
func (m *Map) PutTransientWithMaxIdle(key interface{}, value interface{}, maxIdle time.Duration) error {
	return m.cm.PutTransientWithMaxIdle(context.Background(), key, value, maxIdle)
}

// PutTransientWithTTLAndMaxIdle sets the value for the given key.
// MapStore defined at the server side will not be called.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
// Set maxIdle to 0 for infinite idle time.
func (m *Map) PutTransientWithTTLAndMaxIdle(key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) error {
	return m.cm.PutTransientWithTTLAndMaxIdle(context.Background(), key, value, ttl, maxIdle)
}

// Remove deletes the value for the given key and returns it.
func (m *Map) Remove(key interface{}) (interface{}, error) {
	return m.cm.Remove(context.Background(), key)
}

// RemoveAll deletes all entries matching the given predicate.
func (m *Map) RemoveAll(predicate predicate.Predicate) error {
	return m.cm.RemoveAll(context.Background(), predicate)
}

// RemoveEntryListener removes the specified entry listener.
func (m *Map) RemoveEntryListener(subscriptionID types.UUID) error {
	return m.cm.RemoveEntryListener(context.Background(), subscriptionID)
}

// RemoveInterceptor removes the interceptor.
func (m *Map) RemoveInterceptor(registrationID string) (bool, error) {
	return m.cm.RemoveInterceptor(context.Background(), registrationID)
}

// RemoveIfSame removes the entry for a key only if it is currently mapped to a given value.
// Returns true if the entry was removed.
func (m *Map) RemoveIfSame(key interface{}, value interface{}) (bool, error) {
	return m.cm.RemoveIfSame(context.Background(), key, value)
}

// Replace replaces the entry for a key only if it is currently mapped to some value and returns the previous value.
func (m *Map) Replace(key interface{}, value interface{}) (interface{}, error) {
	return m.cm.Replace(context.Background(), key, value)
}

// ReplaceIfSame replaces the entry for a key only if it is currently mapped to a given value.
// Returns true if the value was replaced.
func (m *Map) ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (bool, error) {
	return m.cm.ReplaceIfSame(context.Background(), key, oldValue, newValue)
}

// Set sets the value for the given key.
func (m *Map) Set(key interface{}, value interface{}) error {
	return m.cm.set(context.Background(), key, value, TtlDefault)
}

// SetTTL updates the TTL value of the entry specified by the given key with a new TTL value.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
func (m *Map) SetTTL(key interface{}, ttl time.Duration) error {
	return m.cm.Set(context.Background(), key, ttl)
}

// SetWithTTL sets the value for the given key.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
func (m *Map) SetWithTTL(key interface{}, value interface{}, ttl time.Duration) error {
	return m.cm.set(context.Background(), key, value, ttl.Milliseconds())
}

// SetWithTTLAndMaxIdle sets the value for the given key.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
// Set maxIdle to 0 for infinite idle time.
func (m *Map) SetWithTTLAndMaxIdle(key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) error {
	return m.cm.SetWithTTLAndMaxIdle(context.Background(), key, value, ttl, maxIdle)
}

// Size returns the number of entries in this map.
func (m *Map) Size() (int, error) {
	return m.cm.Size(context.Background())
}

// TryPut tries to put the given key and value into this map and returns immediately.
func (m *Map) TryPut(key interface{}, value interface{}) (bool, error) {
	return m.cm.tryPut(context.Background(), key, value, 0)
}

// TryPut tries to put the given key and value into this map and waits until operation is completed or timeout is reached.
func (m *Map) TryPutWithTimeout(key interface{}, value interface{}, timeout time.Duration) (bool, error) {
	return m.cm.tryPut(context.Background(), key, value, timeout.Milliseconds())
}

// TryRemove tries to remove the given key from this map and returns immediately.
func (m *Map) TryRemove(key interface{}) (interface{}, error) {
	return m.cm.tryRemove(context.Background(), key, 0)
}

// TryRemoveWithTimeout tries to remove the given key from this map and waits until operation is completed or timeout is reached.
func (m *Map) TryRemoveWithTimeout(key interface{}, timeout time.Duration) (interface{}, error) {
	return m.cm.tryRemove(context.Background(), key, timeout.Milliseconds())
}

type ContextMap struct {
	*proxy
	refIDGenerator *iproxy.ReferenceIDGenerator
}

func newContextMap(p *proxy, refIDGenerator *iproxy.ReferenceIDGenerator) *ContextMap {
	return &ContextMap{
		proxy:          p,
		refIDGenerator: refIDGenerator,
	}
}

// NewLockContext augments the passed parent context with a unique lock ID.
// If passed context is nil, context.Background is used as the parent context.
func (m *ContextMap) NewLockContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, lockIDKey, lockID(m.refIDGenerator.NextID()))
}

// AddEntryListener adds a continuous entry listener to this map.
func (m *ContextMap) AddEntryListener(ctx context.Context, config MapEntryListenerConfig, handler EntryNotifiedHandler) (types.UUID, error) {
	return m.addEntryListener(ctx, config.flags, config.IncludeValue, config.Key, config.Predicate, handler)
}

// AddIndexWithConfig adds an index to this map for the specified entries so that queries can run faster.
func (m *ContextMap) AddIndexWithConfig(ctx context.Context, indexConfig types.IndexConfig) error {
	return m.addIndex(ctx, indexConfig)
}

// AddInterceptor adds an interceptor for this map.
func (m *ContextMap) AddInterceptor(ctx context.Context, interceptor interface{}) (string, error) {
	if interceptorData, err := m.proxy.convertToData(interceptor); err != nil {
		return "", err
	} else {
		request := codec.EncodeMapAddInterceptorRequest(m.name, interceptorData)
		if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
			return "", err
		} else {
			return codec.DecodeMapAddInterceptorResponse(response), nil
		}
	}
}

// Clear deletes all entries one by one and fires related events
func (m *ContextMap) Clear(ctx context.Context) error {
	request := codec.EncodeMapClearRequest(m.name)
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

// ContainsKey returns true if the map contains an entry with the given key
func (m *ContextMap) ContainsKey(ctx context.Context, key interface{}) (bool, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapContainsKeyRequest(m.name, keyData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapContainsKeyResponse(response), nil
		}
	}
}

// ContainsValue returns true if the map contains an entry with the given value
func (m *ContextMap) ContainsValue(ctx context.Context, value interface{}) (bool, error) {
	if valueData, err := m.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapContainsValueRequest(m.name, valueData)
		if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
			return false, err
		} else {
			return codec.DecodeMapContainsValueResponse(response), nil
		}
	}
}

// Delete removes the mapping for a key from this map if it is present
// Unlike remove(object), this operation does not return the removed value, which avoids the serialization cost of
// the returned value. If the removed value will not be used, a delete operation is preferred over a remove
// operation for better performance.
func (m *ContextMap) Delete(ctx context.Context, key interface{}) error {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		request := codec.EncodeMapDeleteRequest(m.name, keyData, lid)
		_, err := m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

// Evict evicts the mapping for a key from this map.
// Returns true if the key is evicted.
func (m *ContextMap) Evict(ctx context.Context, key interface{}) (bool, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapEvictRequest(m.name, keyData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapEvictResponse(response), nil
		}
	}
}

// EvictAll deletes all entries without firing releated events
func (m *ContextMap) EvictAll(ctx context.Context) error {
	request := codec.EncodeMapEvictAllRequest(m.name)
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

// ExecuteOnEntries applies the user defined EntryProcessor to all the entries in the map.
func (m *ContextMap) ExecuteOnEntries(ctx context.Context, entryProcessor interface{}) ([]types.Entry, error) {
	if processorData, err := m.validateAndSerialize(entryProcessor); err != nil {
		return nil, err
	} else {
		ch := make(chan []proto.Pair, 1)
		handler := func(msg *proto.ClientMessage) {
			ch <- codec.DecodeMapExecuteOnAllKeysResponse(msg)
		}
		request := codec.EncodeMapExecuteOnAllKeysRequest(m.name, processorData)
		if _, err := m.invokeOnRandomTarget(ctx, request, handler); err != nil {
			return nil, err
		}
		pairs := <-ch
		if kvPairs, err := m.convertPairsToEntries(pairs); err != nil {
			return nil, err
		} else {
			return kvPairs, nil
		}
	}
}

// Flush flushes all the local dirty entries.
func (m *ContextMap) Flush(ctx context.Context) error {
	request := codec.EncodeMapFlushRequest(m.name)
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

// ForceUnlock releases the lock for the specified key regardless of the lock owner.
// It always successfully unlocks the key, never blocks, and returns immediately.
func (m *ContextMap) ForceUnlock(ctx context.Context, key interface{}) error {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.refIDGenerator.NextID()
		request := codec.EncodeMapForceUnlockRequest(m.name, keyData, refID)
		_, err = m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

// Get returns the value for the specified key, or nil if this map does not contain this key.
// Warning:
// This method returns a clone of original value, modifying the returned value does not change the
// actual value in the map. One should put modified value back to make changes visible to all nodes.
func (m *ContextMap) Get(ctx context.Context, key interface{}) (interface{}, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapGetRequest(m.name, keyData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeMapGetResponse(response))
		}
	}
}

// GetAll returns the entries for the given keys.
func (m *ContextMap) GetAll(ctx context.Context, keys ...interface{}) ([]types.Entry, error) {
	partitionToKeys := map[int32][]*iserialization.Data{}
	ps := m.proxy.partitionService
	for _, key := range keys {
		if keyData, err := m.validateAndSerialize(key); err != nil {
			return nil, err
		} else {
			if partitionKey, err := ps.GetPartitionID(keyData); err != nil {
				return nil, err
			} else {
				arr := partitionToKeys[partitionKey]
				partitionToKeys[partitionKey] = append(arr, keyData)
			}
		}
	}
	result := make([]types.Entry, 0, len(keys))
	// create futures
	f := func(partitionID int32, keys []*iserialization.Data) cb.Future {
		request := codec.EncodeMapGetAllRequest(m.name, keys)
		return m.circuitBreaker.TryContextFuture(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
			return m.invokeOnPartition(ctx, request, partitionID)
		})
	}
	futures := make([]cb.Future, 0, len(partitionToKeys))
	for partitionID, keys := range partitionToKeys {
		futures = append(futures, f(partitionID, keys))
	}
	for _, future := range futures {
		if futureResult, err := future.Result(); err != nil {
			return nil, err
		} else {
			pairs := codec.DecodeMapGetAllResponse(futureResult.(*proto.ClientMessage))
			var key, value interface{}
			var err error
			for _, pair := range pairs {
				if key, err = m.convertToObject(pair.Key().(*iserialization.Data)); err != nil {
					return nil, err
				} else if value, err = m.convertToObject(pair.Value().(*iserialization.Data)); err != nil {
					return nil, err
				}
				result = append(result, types.NewEntry(key, value))
			}
		}
	}
	return result, nil
}

// GetEntrySet returns a clone of the mappings contained in this map.
func (m *ContextMap) GetEntrySet(ctx context.Context) ([]types.Entry, error) {
	request := codec.EncodeMapEntrySetRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return nil, err
	} else {
		return m.convertPairsToEntries(codec.DecodeMapEntrySetResponse(response))
	}
}

// GetEntrySetWithPredicate returns a clone of the mappings contained in this map.
func (m *ContextMap) GetEntrySetWithPredicate(ctx context.Context, predicate predicate.Predicate) ([]types.Entry, error) {
	if predData, err := m.validateAndSerialize(predicate); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapEntriesWithPredicateRequest(m.name, predData)
		if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
			return nil, err
		} else {
			return m.convertPairsToEntries(codec.DecodeMapEntriesWithPredicateResponse(response))
		}
	}
}

// GetEntryView returns the SimpleEntryView for the specified key.
func (m *ContextMap) GetEntryView(ctx context.Context, key string) (*types.SimpleEntryView, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapGetEntryViewRequest(m.name, keyData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			ev, maxIdle := codec.DecodeMapGetEntryViewResponse(response)
			// XXX: creating a new SimpleEntryView here in order to convert key, data and use maxIdle
			deserializedKey, err := m.convertToObject(ev.Key().(*iserialization.Data))
			if err != nil {
				return nil, err
			}
			deserializedValue, err := m.convertToObject(ev.Value().(*iserialization.Data))
			if err != nil {
				return nil, err
			}
			newEntryView := types.NewSimpleEntryView(
				deserializedKey,
				deserializedValue,
				ev.Cost(),
				ev.CreationTime(),
				ev.ExpirationTime(),
				ev.Hits(),
				ev.LastAccessTime(),
				ev.LastStoredTime(),
				ev.LastUpdateTime(),
				ev.Version(),
				ev.Ttl(),
				maxIdle)
			return newEntryView, nil
		}
	}
}

// GetKeySet returns keys contained in this map
func (m *ContextMap) GetKeySet(ctx context.Context) ([]interface{}, error) {
	request := codec.EncodeMapKeySetRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return nil, err
	} else {
		keyDatas := codec.DecodeMapKeySetResponse(response)
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

// GetKeySetWithPredicate returns keys contained in this map
func (m *ContextMap) GetKeySetWithPredicate(ctx context.Context, predicate predicate.Predicate) ([]interface{}, error) {
	if predicateData, err := m.validateAndSerializePredicate(predicate); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapKeySetWithPredicateRequest(m.name, predicateData)
		if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
			return nil, err
		} else {
			return m.convertToObjects(codec.DecodeMapKeySetWithPredicateResponse(response))
		}
	}
}

// GetValues returns a list clone of the values contained in this map
func (m *ContextMap) GetValues(ctx context.Context) ([]interface{}, error) {
	request := codec.EncodeMapValuesRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return nil, err
	} else {
		return m.convertToObjects(codec.DecodeMapValuesResponse(response))
	}
}

// GetValuesWithPredicate returns a list clone of the values contained in this map
func (m *ContextMap) GetValuesWithPredicate(ctx context.Context, predicate predicate.Predicate) ([]interface{}, error) {
	if predicateData, err := m.validateAndSerializePredicate(predicate); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapValuesWithPredicateRequest(m.name, predicateData)
		if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
			return nil, err
		} else {
			return m.convertToObjects(codec.DecodeMapValuesWithPredicateResponse(response))
		}
	}
}

// IsEmpty returns true if this map contains no key-value mappings.
func (m *ContextMap) IsEmpty(ctx context.Context) (bool, error) {
	request := codec.EncodeMapIsEmptyRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return false, err
	} else {
		return codec.DecodeMapIsEmptyResponse(response), nil
	}
}

// IsLocked checks the lock for the specified key.
func (m *ContextMap) IsLocked(ctx context.Context, key interface{}) (bool, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapIsLockedRequest(m.name, keyData)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapIsLockedResponse(response), nil
		}
	}
}

// LoadAllWithoutReplacing loads all keys from the store at server side or loads the given keys if provided.
func (m *ContextMap) LoadAllWithoutReplacing(ctx context.Context, keys ...interface{}) error {
	return m.loadAll(ctx, false, keys...)
}

// LoadAllReplacing loads all keys from the store at server side or loads the given keys if provided.
// Replaces existing keys.
func (m *ContextMap) LoadAllReplacing(ctx context.Context, keys ...interface{}) error {
	return m.loadAll(ctx, true, keys...)
}

// Lock acquires the lock for the specified key infinitely or for the specified lease time if provided.
// If the lock is not available, the current thread becomes disabled for thread scheduling purposes and lies
// dormant until the lock has been acquired.
//
// You get a lock whether the value is present in the map or not. Other threads (possibly on other systems) would
// block on their invoke of lock() until the non-existent key is unlocked. If the lock holder introduces the key to
// the map, the put() operation is not blocked. If a thread not holding a lock on the non-existent key tries to
// introduce the key while a lock exists on the non-existent key, the put() operation blocks until it is unlocked.
//
// Scope of the lock is this map only. Acquired lock is only for the key in this map.
//
// Locks are re-entrant; so, if the key is locked N times, it should be unlocked N times before another thread can
// acquire it.
func (m *ContextMap) Lock(ctx context.Context, key interface{}) error {
	return m.lock(ctx, key, TtlDefault)
}

// LockWithLease acquires the lock for the specified key infinitely or for the specified lease time if provided.
// If the lock is not available, the current thread becomes disabled for thread scheduling purposes and lies
// dormant until the lock has been acquired.
//
// You get a lock whether the value is present in the map or not. Other threads (possibly on other systems) would
// block on their invoke of lock() until the non-existent key is unlocked. If the lock holder introduces the key to
// the map, the put() operation is not blocked. If a thread not holding a lock on the non-existent key tries to
// introduce the key while a lock exists on the non-existent key, the put() operation blocks until it is unlocked.
//
// Scope of the lock is this map only. Acquired lock is only for the key in this map.
//
// Locks are re-entrant; so, if the key is locked N times, it should be unlocked N times before another thread can
// acquire it.
// Lease time is the the time to wait before releasing the lock.
func (m *ContextMap) LockWithLease(ctx context.Context, key interface{}, leaseTime time.Duration) error {
	return m.lock(ctx, key, leaseTime.Milliseconds())
}

// Put sets the value for the given key and returns the old value.
func (m *ContextMap) Put(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	return m.putTTL(ctx, key, value, TtlDefault)
}

// PutWithTTL sets the value for the given key and returns the old value.
// Entry will expire and get evicted after the ttl.
func (m *ContextMap) PutWithTTL(ctx context.Context, key interface{}, value interface{}, ttl time.Duration) (interface{}, error) {
	return m.putTTL(ctx, key, value, ttl.Milliseconds())
}

// PutWithMaxIdle sets the value for the given key and returns the old value.
// maxIdle is the maximum time in seconds for this entry to stay idle in the map.
func (m *ContextMap) PutWithMaxIdle(ctx context.Context, key interface{}, value interface{}, maxIdle time.Duration) (interface{}, error) {
	return m.putMaxIdle(ctx, key, value, TtlDefault, maxIdle.Milliseconds())
}

// PutWithTTLAndMaxIdle sets the value for the given key and returns the old value.
// Entry will expire and get evicted after the ttl.
// maxIdle is the maximum time in seconds for this entry to stay idle in the map.
func (m *ContextMap) PutWithTTLAndMaxIdle(ctx context.Context, key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) (interface{}, error) {
	return m.putMaxIdle(ctx, key, value, ttl.Milliseconds(), maxIdle.Milliseconds())
}

// PutAll copies all of the mappings from the specified map to this map.
// No atomicity guarantees are given. In the case of a failure, some of the key-value tuples may get written,
// while others are not.
func (m *ContextMap) PutAll(ctx context.Context, keyValuePairs ...types.Entry) error {
	f := func(partitionID int32, entries []proto.Pair) cb.Future {
		request := codec.EncodeMapPutAllRequest(m.name, entries, true)
		return m.circuitBreaker.TryContextFuture(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
			return m.invokeOnPartitionAsync(request, partitionID).GetWithContext(ctx)
		})
	}
	return m.putAll(keyValuePairs, f)
}

// PutIfAbsent associates the specified key with the given value if it is not already associated.
func (m *ContextMap) PutIfAbsent(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	return m.putIfAbsent(ctx, key, value, TtlDefault)
}

// PutIfAbsentWithTTL associates the specified key with the given value if it is not already associated.
// Entry will expire and get evicted after the ttl.
func (m *ContextMap) PutIfAbsentWithTTL(ctx context.Context, key interface{}, value interface{}, ttl time.Duration) (interface{}, error) {
	return m.putIfAbsent(ctx, key, value, ttl.Milliseconds())
}

// PutIfAbsentWithTTLAndMaxIdle associates the specified key with the given value if it is not already associated.
// Entry will expire and get evicted after the ttl.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
func (m *ContextMap) PutIfAbsentWithTTLAndMaxIdle(ctx context.Context, key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) (interface{}, error) {
	lid := extractLockID(ctx)
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapPutIfAbsentWithMaxIdleRequest(m.name, keyData, valueData, lid, ttl.Milliseconds(), maxIdle.Milliseconds())
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return codec.DecodeMapPutIfAbsentWithMaxIdleResponse(response), nil
		}
	}
}

// PutTransient sets the value for the given key.
// MapStore defined at the server side will not be called.
// The TTL defined on the server-side configuration will be used.
// Max idle time defined on the server-side configuration will be used.
func (m *ContextMap) PutTransient(ctx context.Context, key interface{}, value interface{}) error {
	return m.putTransient(ctx, key, value, TtlDefault, MaxIdleDefault)
}

// PutTransientWithTTL sets the value for the given key.
// MapStore defined at the server side will not be called.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
func (m *ContextMap) PutTransientWithTTL(ctx context.Context, key interface{}, value interface{}, ttl time.Duration) error {
	return m.putTransient(ctx, key, value, ttl.Milliseconds(), MaxIdleDefault)
}

// PutTransientWithMaxIdle sets the value for the given key.
// MapStore defined at the server side will not be called.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
// Set maxIdle to 0 for infinite idle time.
func (m *ContextMap) PutTransientWithMaxIdle(ctx context.Context, key interface{}, value interface{}, maxIdle time.Duration) error {
	return m.putTransient(ctx, key, value, TtlDefault, maxIdle.Milliseconds())
}

// PutTransientWithTTLAndMaxIdle sets the value for the given key.
// MapStore defined at the server side will not be called.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
// Set maxIdle to 0 for infinite idle time.
func (m *ContextMap) PutTransientWithTTLAndMaxIdle(ctx context.Context, key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) error {
	return m.putTransient(ctx, key, value, ttl.Milliseconds(), maxIdle.Milliseconds())
}

// Remove deletes the value for the given key and returns it.
func (m *ContextMap) Remove(ctx context.Context, key interface{}) (interface{}, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapRemoveRequest(m.name, keyData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeMapRemoveResponse(response))
		}
	}
}

// RemoveAll deletes all entries matching the given predicate.
func (m *ContextMap) RemoveAll(ctx context.Context, predicate predicate.Predicate) error {
	if predicateData, err := m.validateAndSerialize(predicate); err != nil {
		return err
	} else {
		request := codec.EncodeMapRemoveAllRequest(m.name, predicateData)
		_, err := m.invokeOnRandomTarget(ctx, request, nil)
		return err
	}
}

// RemoveEntryListener removes the specified entry listener.
func (m *ContextMap) RemoveEntryListener(ctx context.Context, subscriptionID types.UUID) error {
	return m.listenerBinder.Remove(ctx, subscriptionID)
}

// RemoveInterceptor removes the interceptor.
func (m *ContextMap) RemoveInterceptor(ctx context.Context, registrationID string) (bool, error) {
	request := codec.EncodeMapRemoveInterceptorRequest(m.name, registrationID)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return false, nil
	} else {
		return codec.DecodeMapRemoveInterceptorResponse(response), nil
	}
}

// RemoveIfSame removes the entry for a key only if it is currently mapped to a given value.
// Returns true if the entry was removed.
func (m *ContextMap) RemoveIfSame(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	lid := extractLockID(ctx)
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapRemoveIfSameRequest(m.name, keyData, valueData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapRemoveIfSameResponse(response), nil
		}
	}
}

// Replace replaces the entry for a key only if it is currently mapped to some value and returns the previous value.
func (m *ContextMap) Replace(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	lid := extractLockID(ctx)
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapReplaceRequest(m.name, keyData, valueData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeMapReplaceResponse(response))
		}
	}
}

// ReplaceIfSame replaces the entry for a key only if it is currently mapped to a given value.
// Returns true if the value was replaced.
func (m *ContextMap) ReplaceIfSame(ctx context.Context, key interface{}, oldValue interface{}, newValue interface{}) (bool, error) {
	lid := extractLockID(ctx)
	if keyData, oldValueData, newValueData, err := m.validateAndSerialize3(key, oldValue, newValue); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapReplaceIfSameRequest(m.name, keyData, oldValueData, newValueData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapReplaceIfSameResponse(response), nil
		}
	}
}

// Set sets the value for the given key.
func (m *ContextMap) Set(ctx context.Context, key interface{}, value interface{}) error {
	return m.set(ctx, key, value, TtlDefault)
}

// SetTTL updates the TTL value of the entry specified by the given key with a new TTL value.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
func (m *ContextMap) SetTTL(ctx context.Context, key interface{}, ttl time.Duration) error {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		request := codec.EncodeMapSetTtlRequest(m.name, keyData, ttl.Milliseconds())
		_, err := m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

// SetWithTTL sets the value for the given key.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
func (m *ContextMap) SetWithTTL(ctx context.Context, key interface{}, value interface{}, ttl time.Duration) error {
	return m.set(ctx, key, value, ttl.Milliseconds())
}

// SetWithTTLAndMaxIdle sets the value for the given key.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
// Set maxIdle to 0 for infinite idle time.
func (m *ContextMap) SetWithTTLAndMaxIdle(ctx context.Context, key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) error {
	lid := extractLockID(ctx)
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return err
	} else {
		request := codec.EncodeMapSetWithMaxIdleRequest(m.name, keyData, valueData, lid, ttl.Milliseconds(), maxIdle.Milliseconds())
		_, err := m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

// Size returns the number of entries in this map.
func (m *ContextMap) Size(ctx context.Context) (int, error) {
	request := codec.EncodeMapSizeRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeMapSizeResponse(response)), nil
	}
}

// TryLock tries to acquire the lock for the specified key.
// When the lock is not available, the current thread doesn't wait and returns false immediately.
func (m *ContextMap) TryLock(ctx context.Context, key interface{}) (bool, error) {
	return m.tryLock(ctx, key, 0, 0)
}

// TryLockWithLease tries to acquire the lock for the specified key.
// Lock will be released after lease time passes.
func (m *ContextMap) TryLockWithLease(ctx context.Context, key interface{}, lease time.Duration) (bool, error) {
	return m.tryLock(ctx, key, lease.Milliseconds(), 0)
}

// TryLockWithTimeout tries to acquire the lock for the specified key.
// The current thread becomes disabled for thread scheduling purposes and lies
// dormant until one of the followings happens:
// - The lock is acquired by the current thread, or
// - The specified waiting time elapses.
func (m *ContextMap) TryLockWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (bool, error) {
	return m.tryLock(ctx, key, 0, timeout.Milliseconds())
}

// TryLockWithLeaseTimeout tries to acquire the lock for the specified key.
// The current thread becomes disabled for thread scheduling purposes and lies
// dormant until one of the followings happens:
// - The lock is acquired by the current thread, or
// - The specified waiting time elapses.
// Lock will be released after lease time passes.
func (m *ContextMap) TryLockWithLeaseTimeout(ctx context.Context, key interface{}, lease time.Duration, timeout time.Duration) (bool, error) {
	return m.tryLock(ctx, key, lease.Milliseconds(), timeout.Milliseconds())
}

// TryPut tries to put the given key and value into this map and returns immediately.
func (m *ContextMap) TryPut(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	return m.tryPut(ctx, key, value, 0)
}

// TryPutWithTimeout tries to put the given key and value into this map and waits until operation is completed or timeout is reached.
func (m *ContextMap) TryPutWithTimeout(ctx context.Context, key interface{}, value interface{}, timeout time.Duration) (bool, error) {
	return m.tryPut(ctx, key, value, timeout.Milliseconds())
}

// TryRemove tries to remove the given key from this map and returns immediately.
func (m *ContextMap) TryRemove(ctx context.Context, key interface{}) (interface{}, error) {
	return m.tryRemove(ctx, key, 0)
}

// TryRemoveWithTimeout tries to remove the given key from this map and waits until operation is completed or timeout is reached.
func (m *ContextMap) TryRemoveWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (interface{}, error) {
	return m.tryRemove(ctx, key, timeout.Milliseconds())
}

// Unlock releases the lock for the specified key.
func (m *ContextMap) Unlock(ctx context.Context, key interface{}) error {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.refIDGenerator.NextID()
		request := codec.EncodeMapUnlockRequest(m.name, keyData, lid, refID)
		_, err = m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

func (m *ContextMap) addIndex(ctx context.Context, indexConfig types.IndexConfig) error {
	if err := validateAndNormalizeIndexConfig(&indexConfig); err != nil {
		return err
	}
	request := codec.EncodeMapAddIndexRequest(m.name, indexConfig)
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

func (m *ContextMap) addEntryListener(ctx context.Context, flags int32, includeValue bool, key interface{}, predicate predicate.Predicate, handler EntryNotifiedHandler) (types.UUID, error) {
	var err error
	var keyData *iserialization.Data
	var predicateData *iserialization.Data
	if key != nil {
		if keyData, err = m.validateAndSerialize(key); err != nil {
			return types.UUID{}, err
		}
	}
	if predicate != nil {
		if predicateData, err = m.validateAndSerialize(predicate); err != nil {
			return types.UUID{}, err
		}
	}
	subscriptionID := types.NewUUID()
	addRequest := m.makeListenerRequest(keyData, predicateData, flags, includeValue, m.config.ClusterConfig.SmartRouting)
	listenerHandler := func(msg *proto.ClientMessage) {
		m.makeListenerDecoder(msg, keyData, predicateData, m.makeEntryNotifiedListenerHandler(handler))
	}
	removeRequest := codec.EncodeMapRemoveEntryListenerRequest(m.name, subscriptionID)
	err = m.listenerBinder.Add(ctx, subscriptionID, addRequest, removeRequest, listenerHandler)
	return subscriptionID, err
}

func (m *ContextMap) loadAll(ctx context.Context, replaceExisting bool, keys ...interface{}) error {
	var request *proto.ClientMessage
	if len(keys) == 0 {
		request = codec.EncodeMapLoadAllRequest(m.name, replaceExisting)
	} else {
		keyDatas := make([]*iserialization.Data, 0, len(keys))
		for _, key := range keys {
			if keyData, err := m.convertToData(key); err != nil {
				return err
			} else {
				keyDatas = append(keyDatas, keyData)
			}
		}
		request = codec.EncodeMapLoadGivenKeysRequest(m.name, keyDatas, replaceExisting)
	}
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

func (m *ContextMap) lock(ctx context.Context, key interface{}, ttl int64) error {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.refIDGenerator.NextID()
		request := codec.EncodeMapLockRequest(m.name, keyData, lid, ttl, refID)
		_, err = m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

func (m *ContextMap) putTTL(ctx context.Context, key interface{}, value interface{}, ttl int64) (interface{}, error) {
	lid := extractLockID(ctx)
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapPutRequest(m.name, keyData, valueData, lid, ttl)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeMapPutResponse(response))
		}
	}
}

func (m *ContextMap) putMaxIdle(ctx context.Context, key interface{}, value interface{}, ttl int64, maxIdle int64) (interface{}, error) {
	lid := extractLockID(ctx)
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapPutWithMaxIdleRequest(m.name, keyData, valueData, lid, ttl, maxIdle)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeMapPutWithMaxIdleResponse(response))
		}
	}
}

func (m *ContextMap) putIfAbsent(ctx context.Context, key interface{}, value interface{}, ttl int64) (interface{}, error) {
	lid := extractLockID(ctx)
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapPutIfAbsentRequest(m.name, keyData, valueData, lid, ttl)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return codec.DecodeMapPutIfAbsentResponse(response), nil
		}
	}
}

func (m *ContextMap) putTransient(ctx context.Context, key interface{}, value interface{}, ttl int64, maxIdle int64) error {
	lid := extractLockID(ctx)
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return err
	} else {
		var request *proto.ClientMessage
		if maxIdle >= 0 {
			request = codec.EncodeMapPutTransientWithMaxIdleRequest(m.name, keyData, valueData, lid, ttl, maxIdle)
		} else {
			request = codec.EncodeMapPutTransientRequest(m.name, keyData, valueData, lid, ttl)
		}
		_, err = m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

func (m *ContextMap) tryLock(ctx context.Context, key interface{}, lease int64, timeout int64) (bool, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		refID := m.refIDGenerator.NextID()
		request := codec.EncodeMapTryLockRequest(m.name, keyData, lid, lease, timeout, refID)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapTryLockResponse(response), nil
		}
	}
}

func (m *ContextMap) makeListenerRequest(keyData, predicateData *iserialization.Data, flags int32, includeValue bool, smart bool) *proto.ClientMessage {
	if keyData != nil {
		if predicateData != nil {
			return codec.EncodeMapAddEntryListenerToKeyWithPredicateRequest(m.name, keyData, predicateData, includeValue, flags, smart)
		} else {
			return codec.EncodeMapAddEntryListenerToKeyRequest(m.name, keyData, includeValue, flags, m.config.ClusterConfig.SmartRouting)
		}
	} else if predicateData != nil {
		return codec.EncodeMapAddEntryListenerWithPredicateRequest(m.name, predicateData, includeValue, flags, m.config.ClusterConfig.SmartRouting)
	} else {
		return codec.EncodeMapAddEntryListenerRequest(m.name, includeValue, flags, m.config.ClusterConfig.SmartRouting)
	}
}

func (m *ContextMap) makeListenerDecoder(msg *proto.ClientMessage, keyData, predicateData *iserialization.Data, handler entryNotifiedHandler) {
	if keyData != nil {
		if predicateData != nil {
			codec.HandleMapAddEntryListenerToKeyWithPredicate(msg, handler)
		} else {
			codec.HandleMapAddEntryListenerToKey(msg, handler)
		}
	} else if predicateData != nil {
		codec.HandleMapAddEntryListenerWithPredicate(msg, handler)
	} else {
		codec.HandleMapAddEntryListener(msg, handler)
	}
}

func (m *ContextMap) set(ctx context.Context, key interface{}, value interface{}, ttl int64) error {
	lid := extractLockID(ctx)
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return err
	} else {
		request := codec.EncodeMapSetRequest(m.name, keyData, valueData, lid, ttl)
		_, err := m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

func (m *ContextMap) tryPut(ctx context.Context, key interface{}, value interface{}, timeout int64) (bool, error) {
	lid := extractLockID(ctx)
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapTryPutRequest(m.name, keyData, valueData, lid, timeout)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapTryPutResponse(response), nil
		}
	}
}

func (m *ContextMap) tryRemove(ctx context.Context, key interface{}, timeout int64) (interface{}, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapTryRemoveRequest(m.name, keyData, lid, timeout)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return codec.DecodeMapTryRemoveResponse(response), nil
		}
	}
}

type indexValidationError struct {
	Err error
}

func (ic indexValidationError) Error() string {
	return ic.Err.Error()
}

func validateAndNormalizeIndexConfig(ic *types.IndexConfig) error {
	attrSet := newAttributeSet()
	for _, attr := range ic.Attributes {
		if err := attrSet.Add(attr); err != nil {
			return err
		}
	}
	attrs := attrSet.Attrs()
	if len(attrs) == 0 {
		return &IndexValidationError{errors.New("index must have at least one attribute")}
	}
	if len(attrs) > maxIndexAttributes {
		return &IndexValidationError{fmt.Errorf("index cannot have more than %d attributes", maxIndexAttributes)}
	}
	if ic.Type == types.IndexTypeBitmap && len(attrs) > 1 {
		return &IndexValidationError{errors.New("composite bitmap indexes are not supported")}
	}
	ic.Attributes = attrs
	return nil
}

type attributeSet struct {
	attrs map[string]struct{}
}

func newAttributeSet() attributeSet {
	return attributeSet{map[string]struct{}{}}
}

func (as attributeSet) Add(attr string) error {
	if attr == "" {
		return &IndexValidationError{errors.New("attribute name cannot be not empty")}
	}
	if strings.HasSuffix(attr, ".") {
		return &IndexValidationError{fmt.Errorf("attribute name cannot end with dot: %s", attr)}
	}
	if strings.HasPrefix(attr, "this.") {
		attr = strings.Replace(attr, "this.", "", 1)
		if attr == "" {
			return &IndexValidationError{errors.New("attribute name cannot be 'this.'")}
		}
	}
	if _, ok := as.attrs[attr]; ok {
		return &IndexValidationError{fmt.Errorf("duplicate attribute name not allowed: %s", attr)}
	}
	as.attrs[attr] = struct{}{}
	return nil
}

func (as attributeSet) Attrs() []string {
	attrs := make([]string, 0, len(as.attrs))
	for attr := range as.attrs {
		attrs = append(attrs, attr)
	}
	return attrs
}

type MapEntryListenerConfig struct {
	Predicate    predicate.Predicate
	Key          interface{}
	flags        int32
	IncludeValue bool
}

func (c *MapEntryListenerConfig) NotifyEntryAdded(enable bool) {
	flagsSetOrClear(&c.flags, NotifyEntryAdded, enable)
}

func (c *MapEntryListenerConfig) NotifyEntryRemoved(enable bool) {
	flagsSetOrClear(&c.flags, NotifyEntryRemoved, enable)
}

func (c *MapEntryListenerConfig) NotifyEntryUpdated(enable bool) {
	flagsSetOrClear(&c.flags, NotifyEntryUpdated, enable)
}

func (c *MapEntryListenerConfig) NotifyEntryEvicted(enable bool) {
	flagsSetOrClear(&c.flags, NotifyEntryEvicted, enable)
}

func (c *MapEntryListenerConfig) NotifyEntryExpired(enable bool) {
	flagsSetOrClear(&c.flags, NotifyEntryExpired, enable)
}

func (c *MapEntryListenerConfig) NotifyEntryAllEvicted(enable bool) {
	flagsSetOrClear(&c.flags, NotifyEntryAllEvicted, enable)
}

func (c *MapEntryListenerConfig) NotifyEntryAllCleared(enable bool) {
	flagsSetOrClear(&c.flags, NotifyEntryAllCleared, enable)
}

func (c *MapEntryListenerConfig) NotifyEntryMerged(enable bool) {
	flagsSetOrClear(&c.flags, NotifyEntryMerged, enable)
}

func (c *MapEntryListenerConfig) NotifyEntryInvalidated(enable bool) {
	flagsSetOrClear(&c.flags, NotifyEntryInvalidated, enable)
}

func (c *MapEntryListenerConfig) NotifyEntryLoaded(enable bool) {
	flagsSetOrClear(&c.flags, NotifyEntryLoaded, enable)
}

func flagsSetOrClear(flags *int32, flag int32, enable bool) {
	if enable {
		*flags |= flag
	} else {
		*flags &^= flag
	}
}

// extractLockID extracts lock ID from the context.
// If the lock ID is not found, it returns the default lock ID.
func extractLockID(ctx context.Context) int64 {
	if ctx == nil {
		panic("context is nil")
	}
	if lockIDValue := ctx.Value(lockIDKey); lockIDValue == nil {
		return defaultLockID
	} else if lid, ok := lockIDValue.(lockID); !ok {
		return defaultLockID
	} else {
		return int64(lid)
	}
}
