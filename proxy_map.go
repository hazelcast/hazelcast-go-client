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

package hazelcast

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hazelcast/hazelcast-go-client/aggregate"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/types"
)

/*
Map is a distributed map.
Hazelcast Go client enables you to perform operations like reading and writing from/to a Hazelcast Map with methods like Get and Put.
For details, see https://docs.hazelcast.com/imdg/latest/data-structures/map.html

# Listening for Map Events

To listen events of a map, you can use the AddListener, AddListenerWithKey, AddListenerWithPredicate and AddListenerWithPredicateAndKey methods.
The first method adds a listener to the map's all events. The others filter the events depending on a key and/or a predicate.
In all methods you specify whether you want to include value in the event or not.

You can pass a MapListener struct to these methods to add handlers to different event types. You can add different handlers to different event types
with a single MapListener struct. If you don't specify a handler in for an event type in MapListener struct, there will be no handler for that event.
In the example below, a listener for added and updated entry events is created. Entries only with key "somekey" and matching to predicate year > 2000 are considered:

	entryListenerConfig := hazelcast.MapEntryListenerConfig{
		Key: "somekey",
		Predicate: predicate.Greater("year", 2000),
		IncludeValue: true,
	}

	m, err := client.GetMap(ctx, "somemap")
	// error checking is omitted.
	subscriptionID, err := m.AddListenerWithPredicateAndKey(ctx, hazelcast.MapListener{
		EntryAdded: func(event *hazelcast.EntryNotified) {
			fmt.Println("Entry Added:", event.Value)
		},
		EntryRemoved: func(event *hazelcast.EntryNotified) {
			fmt.Println("Entry Removed:", event.Value)
		},
	}, predicate.Greater("year", 2000), "somekey", true)
	// error checking is omitted.

Adding an event listener returns a subscription ID, which you can later use to remove the listener:

	err = m.RemoveListener(ctx, subscriptionID)

# Using Locks

You can lock entries in a Map.
When an entry is locked, only the owner of that lock can access that entry in the cluster until it is unlocked by the owner of force unlocked.
See https://docs.hazelcast.com/imdg/latest/data-structures/map.html#locking-maps for details.

Locks are reentrant.
The owner of a lock can acquire the lock again without waiting for the lock to be unlocked.
If the key is locked N times, it should be unlocked N times before another goroutine can acquire it.

Lock ownership in Hazelcast Go Client is explicit.
The first step to own a lock is creating a lock context, which is similar to a key.
The lock context is a regular context.Context which carry a special value that uniquely identifies the lock context in the cluster.
Once the lock context is created, it can be used to lock/unlock entries and used with any function that is lock aware, such as Put.

	m, err := client.GetMap(ctx, "my-map")
	lockCtx := m.NewLockContext(ctx)
	// block acquiring the lock
	err = m.Lock(lockCtx, "some-key")
	// pass lock context to use the locked entry
	err = m.Set(lockCtx, "some-key", "some-value")
	// release the lock once done with it
	err = m.Unlock(lockCtx, "some-key")

As mentioned before, lock context is a regular context.Context which carry a special lock ID.
You can pass any context.Context to any Map function, but in that case lock ownership between operations using the same hazelcast.Client instance is not possible.

# Using the Near Cache

Map entries in Hazelcast are partitioned across the cluster members.
Hazelcast clients do not have local data at all.
Suppose you read the key k a number of times from a Hazelcast client or k is owned by another member in your cluster.
Then each map.Get(k) will be a remote operation, which creates a lot of network trips.
If you have a data structure that is mostly read, then you should consider creating a local Near Cache, so that reads are sped up and less network traffic is created.

These benefits do not come for free. See the following trade-offs:

  - Clients with a Near Cache has to hold the extra cached data, which increases memory consumption.
  - If invalidation is enabled and entries are updated frequently, then invalidations will be costly.
  - Near Cache breaks the strong consistency guarantees; you might be reading stale data.

Near Cache is highly recommended for data structures that are mostly read.

You must enable the Near Cache on the client, without the need to configure it on the server.
Note that Near Cache configuration is specific to the server or client itself.
A data structure on a server may not have Near Cache configured while the same data structure on a client may have Near Cache configured.
They also can have different Near Cache configurations.

If you are using the Near Cache, you should take into account that your hits to the keys in the Near Cache are not reflected as hits to the original keys on the primary members.
This has for example an impact on Map's maximum idle seconds or time-to-live seconds expiration.
Therefore, even though there is a hit on a key in Near Cache, your original key on the primary member may expire.

Note: Near Cache works only when you access data via map.Get(k).
Data returned using a predicate or an SQL query is not stored in the Near Cache.

Checkout the nearcache package for configuration options.

Warning: Storing keys in serialized form is required when the key cannot be compared for equality, such as slices.
That can be accomplished by setting SerializeKeys: true, shown in the example below:

	ncc := nearcache.Config{
		Name: "mymap*",
		SerializeKeys: true
	}

The following types cannot be used as keys without setting SerializeKeys==true:

  - Maps
  - Slices
  - Structs with having at least one field with an incomparable type.

Following Map methods support the Near Cache:

  - Clear
  - ContainsKey
  - Delete
  - Evict
  - EvictAll
  - ExecuteOnKey
  - ExecuteOnKeys
  - Get
  - GetAll
  - LoadAllReplacing
  - LoadAllWithoutReplacing
  - LocalMapStats
  - Put
  - PutWithMaxIdle
  - PutWithTTL
  - PutWithTTLAndMaxIdle
  - PutAll
  - PutIfAbsent
  - PutIfAbsentWithTTL
  - PutIfAbsentWithTTLAndMaxIdle
  - PutTransient
  - PutTransientWithMaxIdle
  - PutTransientWithTTL
  - PutTransientWithTTLAndMaxIdle
  - Remove
  - RemoveIfSame
  - RemoveAll
  - Replace
  - ReplaceIfSame
  - Set
  - SetWithTTL
  - SetWithTTLAndMaxIdle
  - TryPut
  - TryPutWithTimeout
  - TryRemove
  - TryRemoveWithTimeout
*/
type Map struct {
	*proxy
	ncm          nearCacheMap
	hasNearCache bool
}

func newMap(p *proxy) *Map {
	return &Map{proxy: p}
}

// NewLockContext augments the passed parent context with a unique lock ID.
// If passed context is nil, context.Background is used as the parent context.
func (m *Map) NewLockContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, lockIDKey{}, lockID(m.refIDGen.NextID()))
}

type MapListener struct {
	EntryAdded   func(event *EntryNotified)
	EntryRemoved func(event *EntryNotified)
	EntryUpdated func(event *EntryNotified)
	EntryEvicted func(event *EntryNotified)
	EntryExpired func(event *EntryNotified)
	MapEvicted   func(event *EntryNotified)
	MapCleared   func(event *EntryNotified)
	EntryMerged  func(event *EntryNotified)
	EntryLoaded  func(event *EntryNotified)
}

func (m *Map) prepareFlagsOfMapListener(listener MapListener) int32 {
	var flags int32
	if listener.EntryAdded != nil {
		flagsSetOrClear(&flags, int32(EntryAdded), true)
	}
	if listener.EntryEvicted != nil {
		flagsSetOrClear(&flags, int32(EntryEvicted), true)
	}
	if listener.EntryExpired != nil {
		flagsSetOrClear(&flags, int32(EntryExpired), true)
	}
	if listener.EntryLoaded != nil {
		flagsSetOrClear(&flags, int32(EntryLoaded), true)
	}
	if listener.EntryMerged != nil {
		flagsSetOrClear(&flags, int32(EntryMerged), true)
	}
	if listener.EntryRemoved != nil {
		flagsSetOrClear(&flags, int32(EntryRemoved), true)
	}
	if listener.EntryUpdated != nil {
		flagsSetOrClear(&flags, int32(EntryUpdated), true)
	}
	if listener.MapCleared != nil {
		flagsSetOrClear(&flags, int32(EntryAllCleared), true)
	}
	if listener.MapEvicted != nil {
		flagsSetOrClear(&flags, int32(EntryAllEvicted), true)
	}
	return flags
}

func (m *Map) mapListenerEventHandler(listener MapListener) EntryNotifiedHandler {
	return func(event *EntryNotified) {
		switch event.EventType {
		case EntryAdded:
			listener.EntryAdded(event)
		case EntryUpdated:
			listener.EntryUpdated(event)
		case EntryRemoved:
			listener.EntryRemoved(event)
		case EntryEvicted:
			listener.EntryEvicted(event)
		case EntryExpired:
			listener.EntryExpired(event)
		case EntryMerged:
			listener.EntryMerged(event)
		case EntryLoaded:
			listener.EntryLoaded(event)
		case EntryAllCleared:
			listener.MapCleared(event)
		case EntryAllEvicted:
			listener.MapEvicted(event)
		default:
			m.logger.Warnf("Not a known map event type: %d", event.EventType)
		}
	}
}

// AddListener adds a continuous entry listener to this map.
func (m *Map) AddListener(ctx context.Context, listener MapListener, includeValue bool) (types.UUID, error) {
	flags := m.prepareFlagsOfMapListener(listener)
	return m.addEntryListener(ctx, flags, includeValue, nil, nil, m.mapListenerEventHandler(listener))
}

// AddListenerWithKey adds a continuous entry listener on a specific key to this map.
func (m *Map) AddListenerWithKey(ctx context.Context, listener MapListener, key interface{}, includeValue bool) (types.UUID, error) {
	flags := m.prepareFlagsOfMapListener(listener)
	return m.addEntryListener(ctx, flags, includeValue, key, nil, m.mapListenerEventHandler(listener))
}

// AddListenerWithPredicate adds a continuous entry listener to this map. Events are filtered by a predicate.
func (m *Map) AddListenerWithPredicate(ctx context.Context, listener MapListener, predicate predicate.Predicate, includeValue bool) (types.UUID, error) {
	flags := m.prepareFlagsOfMapListener(listener)
	return m.addEntryListener(ctx, flags, includeValue, nil, predicate, m.mapListenerEventHandler(listener))
}

// AddListenerWithPredicateAndKey adds a continuous entry listener on a specific key to this map. Events are filtered by a predicate.
func (m *Map) AddListenerWithPredicateAndKey(ctx context.Context, listener MapListener, predicate predicate.Predicate, key interface{}, includeValue bool) (types.UUID, error) {
	flags := m.prepareFlagsOfMapListener(listener)
	return m.addEntryListener(ctx, flags, includeValue, key, predicate, m.mapListenerEventHandler(listener))
}

// AddEntryListener adds a continuous entry listener to this map.
// Deprecated: In favor of AddListener, AddListenerWithKey, AddListenerWithPredicate,
// AddListenerWithPredicateAndKey methods.
func (m *Map) AddEntryListener(ctx context.Context, config MapEntryListenerConfig, handler EntryNotifiedHandler) (types.UUID, error) {
	return m.addEntryListener(ctx, config.flags, config.IncludeValue, config.Key, config.Predicate, handler)
}

// AddIndex adds an index to this map for the specified entries so that queries can run faster.
func (m *Map) AddIndex(ctx context.Context, indexConfig types.IndexConfig) error {
	return m.addIndex(ctx, indexConfig)
}

// AddInterceptor adds an interceptor for this map.
func (m *Map) AddInterceptor(ctx context.Context, interceptor interface{}) (string, error) {
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

// Aggregate runs the given aggregator and returns the result.
func (m *Map) Aggregate(ctx context.Context, agg aggregate.Aggregator) (interface{}, error) {
	aggData, err := m.validateAndSerializeAggregate(agg)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeMapAggregateRequest(m.name, aggData)
	return m.aggregate(ctx, request, codec.DecodeMapAggregateResponse)
}

// AggregateWithPredicate runs the given aggregator and returns the result.
// The result is filtered with the given predicate.
func (m *Map) AggregateWithPredicate(ctx context.Context, agg aggregate.Aggregator, pred predicate.Predicate) (interface{}, error) {
	aggData, err := m.validateAndSerializeAggregate(agg)
	if err != nil {
		return nil, err
	}
	predData, err := m.validateAndSerializePredicate(pred)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeMapAggregateWithPredicateRequest(m.name, aggData, predData)
	return m.aggregate(ctx, request, codec.DecodeMapAggregateWithPredicateResponse)
}

// Clear deletes all entries one by one and fires related events.
func (m *Map) Clear(ctx context.Context) error {
	if m.hasNearCache {
		return m.ncm.Clear(ctx, m)
	}
	return m.clearFromRemote(ctx)
}

// ContainsKey returns true if the map contains an entry with the given key.
func (m *Map) ContainsKey(ctx context.Context, key interface{}) (bool, error) {
	if m.hasNearCache {
		return m.ncm.ContainsKey(ctx, key, m)
	}
	return m.containsKeyFromRemote(ctx, key)
}

// ContainsValue returns true if the map contains an entry with the given value.
func (m *Map) ContainsValue(ctx context.Context, value interface{}) (bool, error) {
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

// Delete removes the mapping for a key from this map if it is present.
// Unlike remove(object), this operation does not return the removed value, which avoids the serialization cost of
// the returned value. If the removed value will not be used, a delete operation is preferred over a remove
// operation for better performance.
func (m *Map) Delete(ctx context.Context, key interface{}) error {
	if m.hasNearCache {
		return m.ncm.Delete(ctx, m, key)
	}
	return m.deleteFromRemote(ctx, key)
}

// Evict evicts the mapping for a key from this map.
// Returns true if the key is evicted.
func (m *Map) Evict(ctx context.Context, key interface{}) (bool, error) {
	if m.hasNearCache {
		return m.ncm.Evict(ctx, m, key)
	}
	return m.evictFromRemote(ctx, key)
}

// EvictAll deletes all entries without firing related events.
func (m *Map) EvictAll(ctx context.Context) error {
	if m.hasNearCache {
		return m.ncm.EvictAll(ctx, m)
	}
	return m.evictAllFromRemote(ctx)
}

// ExecuteOnEntries applies the user defined EntryProcessor to all the entries in the map.
func (m *Map) ExecuteOnEntries(ctx context.Context, entryProcessor interface{}) ([]types.Entry, error) {
	processorData, err := m.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeMapExecuteOnAllKeysRequest(m.name, processorData)
	resp, err := m.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	pairs := codec.DecodeMapExecuteOnAllKeysResponse(resp)
	kvPairs, err := m.convertPairsToEntries(pairs)
	if err != nil {
		return nil, err
	}
	return kvPairs, nil
}

// ExecuteOnKey applies the user defined EntryProcessor to the entry with the specified key in the map.
func (m *Map) ExecuteOnKey(ctx context.Context, entryProcessor interface{}, key interface{}) (interface{}, error) {
	if m.hasNearCache {
		return m.ncm.ExecuteOnKey(ctx, m, entryProcessor, key)
	}
	return m.executeOnKeyFromRemote(ctx, entryProcessor, key)
}

// ExecuteOnKeys applies the user defined EntryProcessor to the entries with the specified keys in the map.
func (m *Map) ExecuteOnKeys(ctx context.Context, entryProcessor interface{}, keys ...interface{}) ([]interface{}, error) {
	if m.hasNearCache {
		return m.ncm.ExecuteOnKeys(ctx, m, entryProcessor, keys)
	}
	return m.executeOnKeysFromRemote(ctx, entryProcessor, keys)
}

// ExecuteOnEntriesWithPredicate applies the user defined EntryProcessor to all the entries in the map which satisfies the predicate.
func (m *Map) ExecuteOnEntriesWithPredicate(ctx context.Context, entryProcessor interface{}, pred predicate.Predicate) ([]types.Entry, error) {
	processorData, err := m.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	predData, err := m.validateAndSerializePredicate(pred)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeMapExecuteWithPredicateRequest(m.name, processorData, predData)
	resp, err := m.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	pairs := codec.DecodeMapExecuteWithPredicateResponse(resp)
	kvPairs, err := m.convertPairsToEntries(pairs)
	if err != nil {
		return nil, err
	}
	return kvPairs, nil
}

// Flush flushes all the local dirty entries.
func (m *Map) Flush(ctx context.Context) error {
	request := codec.EncodeMapFlushRequest(m.name)
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

// ForceUnlock releases the lock for the specified key regardless of the lock owner.
// It always successfully unlocks the key, never blocks, and returns immediately.
func (m *Map) ForceUnlock(ctx context.Context, key interface{}) error {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.refIDGen.NextID()
		request := codec.EncodeMapForceUnlockRequest(m.name, keyData, refID)
		_, err = m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

// Get returns the value for the specified key, or nil if this map does not contain this key.
// Warning: This method returns a clone of original value, modifying the returned value does not change the actual value in the map.
// One should put modified value back to make changes visible to all nodes.
func (m *Map) Get(ctx context.Context, key interface{}) (interface{}, error) {
	if m.hasNearCache {
		return m.ncm.Get(ctx, m, key)
	}
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	return m.getFromRemote(ctx, keyData)
}

func (m *Map) clearFromRemote(ctx context.Context) error {
	request := codec.EncodeMapClearRequest(m.name)
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

func (m *Map) containsKeyFromRemote(ctx context.Context, key interface{}) (bool, error) {
	lid := extractLockID(ctx)
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := codec.EncodeMapContainsKeyRequest(m.name, keyData, lid)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return false, err
	}
	return codec.DecodeMapContainsKeyResponse(response), nil
}

func (m *Map) evictFromRemote(ctx context.Context, key interface{}) (bool, error) {
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	lid := extractLockID(ctx)
	request := codec.EncodeMapEvictRequest(m.name, keyData, lid)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return false, err
	}
	return codec.DecodeMapEvictResponse(response), nil
}

func (m *Map) evictAllFromRemote(ctx context.Context) error {
	request := codec.EncodeMapEvictAllRequest(m.name)
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

func (m *Map) executeOnKeyFromRemote(ctx context.Context, entryProcessor interface{}, key interface{}) (interface{}, error) {
	processorData, err := m.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	lid := extractLockID(ctx)
	request := codec.EncodeMapExecuteOnKeyRequest(m.name, processorData, keyData, lid)
	resp, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return nil, err
	}
	return m.convertToObject(codec.DecodeMapExecuteOnKeyResponse(resp))
}

func (m *Map) executeOnKeysFromRemote(ctx context.Context, entryProcessor interface{}, keys []interface{}) ([]interface{}, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	processorData, err := m.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	keysDataList, err := m.convertToDataList(keys)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeMapExecuteOnKeysRequest(m.name, processorData, keysDataList)
	resp, err := m.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	pairs := codec.DecodeMapExecuteOnKeysResponse(resp)
	return m.convertPairsToValues(pairs)
}

func (m *Map) getFromRemote(ctx context.Context, keyData serialization.Data) (interface{}, error) {
	lid := extractLockID(ctx)
	request := codec.EncodeMapGetRequest(m.name, keyData, lid)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return nil, err
	}
	return m.convertToObject(codec.DecodeMapGetResponse(response))
}

func (m *Map) getAllFromRemote(ctx context.Context, keyCount int, partitionToKeys map[int32][]serialization.Data) ([]proto.Pair, error) {
	futures := make([]cb.Future, 0, len(partitionToKeys))
	for pid, ks := range partitionToKeys {
		request := codec.EncodeMapGetAllRequest(m.name, ks)
		fut := m.invoker.CB().TryContextFuture(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
			if attempt > 0 {
				request = request.Copy()
			}
			return m.invokeOnPartition(ctx, request, pid)
		})
		futures = append(futures, fut)
	}
	result := make([]proto.Pair, 0, keyCount)
	for _, fut := range futures {
		fr, err := fut.Result()
		if err != nil {
			return nil, err
		}
		pairs := codec.DecodeMapGetAllResponse(fr.(*proto.ClientMessage))
		result = append(result, pairs...)
	}
	return result, nil
}

func (m *Map) deleteFromRemote(ctx context.Context, key interface{}) error {
	lid := extractLockID(ctx)
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := codec.EncodeMapDeleteRequest(m.name, keyData, lid)
	if _, err := m.invokeOnKey(ctx, request, keyData); err != nil {
		return err
	}
	return nil
}

func (m *Map) loadAllFromRemote(ctx context.Context, replaceExisting bool, keys []interface{}) error {
	var request *proto.ClientMessage
	if len(keys) == 0 {
		request = codec.EncodeMapLoadAllRequest(m.name, replaceExisting)
	} else {
		keyDatas, err := m.convertToDataList(keys)
		if err != nil {
			return err
		}
		request = codec.EncodeMapLoadGivenKeysRequest(m.name, keyDatas, replaceExisting)
	}
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

func (m *Map) putAllFromRemote(ctx context.Context, entries []types.Entry) error {
	f := func(partitionID int32, entries []proto.Pair) cb.Future {
		request := codec.EncodeMapPutAllRequest(m.name, entries, true)
		now := time.Now()
		return m.invoker.CB().TryContextFuture(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
			if attempt > 0 {
				request = request.Copy()
			}
			if inv, err := m.invokeOnPartitionAsync(ctx, request, partitionID, now); err != nil {
				return nil, err
			} else {
				return inv.GetWithContext(ctx)
			}
		})
	}
	return m.putAll(entries, f)
}

func (m *Map) putWithTTLFromRemote(ctx context.Context, key, value interface{}, ttl int64) (interface{}, error) {
	lid := extractLockID(ctx)
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := codec.EncodeMapPutRequest(m.name, keyData, valueData, lid, ttl)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return nil, err
	}
	return m.convertToObject(codec.DecodeMapPutResponse(response))
}
func (m *Map) putWithMaxIdleFromRemote(ctx context.Context, key, value interface{}, ttl int64, maxIdle int64) (interface{}, error) {
	lid := extractLockID(ctx)
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := codec.EncodeMapPutWithMaxIdleRequest(m.name, keyData, valueData, lid, ttl, maxIdle)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return nil, err
	}
	return m.convertToObject(codec.DecodeMapPutWithMaxIdleResponse(response))
}

func (m *Map) putTransientWithTTLFromRemote(ctx context.Context, key, value interface{}, ttl int64) error {
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	lid := extractLockID(ctx)
	request := codec.EncodeMapPutTransientRequest(m.name, keyData, valueData, lid, ttl)
	_, err = m.invokeOnKey(ctx, request, keyData)
	return err
}

func (m *Map) putTransientWithTTLAndMaxIdleFromRemote(ctx context.Context, key interface{}, value interface{}, ttl int64, maxIdle int64) error {
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	lid := extractLockID(ctx)
	request := codec.EncodeMapPutTransientWithMaxIdleRequest(m.name, keyData, valueData, lid, ttl, maxIdle)
	_, err = m.invokeOnKey(ctx, request, keyData)
	return err
}

func (m *Map) putIfAbsentWithTTLFromRemote(ctx context.Context, key interface{}, value interface{}, ttl int64) (interface{}, error) {
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	lid := extractLockID(ctx)
	request := codec.EncodeMapPutIfAbsentRequest(m.name, keyData, valueData, lid, ttl)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return nil, err
	}
	return m.convertToObject(codec.DecodeMapPutIfAbsentResponse(response))
}

func (m *Map) putIfAbsentWithTTLAndMaxIdleFromRemote(ctx context.Context, key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) (interface{}, error) {
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	lid := extractLockID(ctx)
	request := codec.EncodeMapPutIfAbsentWithMaxIdleRequest(m.name, keyData, valueData, lid, ttl.Milliseconds(), maxIdle.Milliseconds())
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return nil, err
	}
	return m.convertToObject(codec.DecodeMapPutIfAbsentWithMaxIdleResponse(response))
}

func (m *Map) removeFromRemote(ctx context.Context, key interface{}) (interface{}, error) {
	lid := extractLockID(ctx)
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeMapRemoveRequest(m.name, keyData, lid)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return nil, err
	}
	return m.convertToObject(codec.DecodeMapRemoveResponse(response))
}

func (m *Map) removeAllFromRemote(ctx context.Context, predicate predicate.Predicate) error {
	predicateData, err := m.validateAndSerialize(predicate)
	if err != nil {
		return err
	}
	request := codec.EncodeMapRemoveAllRequest(m.name, predicateData)
	_, err = m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

func (m *Map) replaceFromRemote(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	lid := extractLockID(ctx)
	request := codec.EncodeMapReplaceRequest(m.name, keyData, valueData, lid)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return nil, err
	}
	return m.convertToObject(codec.DecodeMapReplaceResponse(response))
}

func (m *Map) replaceIfSameFromRemote(ctx context.Context, key interface{}, oldValue interface{}, newValue interface{}) (bool, error) {
	lid := extractLockID(ctx)
	keyData, oldValueData, newValueData, err := m.validateAndSerialize3(key, oldValue, newValue)
	if err != nil {
		return false, err
	}
	request := codec.EncodeMapReplaceIfSameRequest(m.name, keyData, oldValueData, newValueData, lid)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return false, err
	}
	return codec.DecodeMapReplaceIfSameResponse(response), nil
}

func (m *Map) removeIfSameFromRemote(ctx context.Context, key, value interface{}) (bool, error) {
	lid := extractLockID(ctx)
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := codec.EncodeMapRemoveIfSameRequest(m.name, keyData, valueData, lid)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return false, err
	}
	return codec.DecodeMapRemoveIfSameResponse(response), nil
}

func (m *Map) setFromRemote(ctx context.Context, key, value interface{}, ttl int64) error {
	lid := extractLockID(ctx)
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	request := codec.EncodeMapSetRequest(m.name, keyData, valueData, lid, ttl)
	if _, err := m.invokeOnKey(ctx, request, keyData); err != nil {
		return err
	}
	return nil
}

func (m *Map) setWithTTLAndMaxIdleFromRemote(ctx context.Context, key, value interface{}, ttl time.Duration, maxIdle time.Duration) error {
	lid := extractLockID(ctx)
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	request := codec.EncodeMapSetWithMaxIdleRequest(m.name, keyData, valueData, lid, ttl.Milliseconds(), maxIdle.Milliseconds())
	if _, err := m.invokeOnKey(ctx, request, keyData); err != nil {
		return err
	}
	return nil
}

func (m *Map) tryRemoveFromRemote(ctx context.Context, key interface{}, timeout int64) (interface{}, error) {
	lid := extractLockID(ctx)
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeMapTryRemoveRequest(m.name, keyData, lid, timeout)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return nil, err
	}
	return codec.DecodeMapTryRemoveResponse(response), nil
}

func (m *Map) tryPutFromRemote(ctx context.Context, key interface{}, value interface{}, timeout int64) (bool, error) {
	lid := extractLockID(ctx)
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := codec.EncodeMapTryPutRequest(m.name, keyData, valueData, lid, timeout)
	response, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return false, err
	}
	return codec.DecodeMapTryPutResponse(response), nil
}

// GetAll returns the entries for the given keys.
func (m *Map) GetAll(ctx context.Context, keys ...interface{}) ([]types.Entry, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if m.hasNearCache {
		return m.ncm.GetAll(ctx, m, keys)
	}
	return m.getAll(ctx, keys)
}

func (m *Map) getAll(ctx context.Context, keys []interface{}) ([]types.Entry, error) {
	partitionToKeys, err := m.partitionToKeys(keys, false)
	if err != nil {
		return nil, err
	}
	pairs, err := m.getAllFromRemote(ctx, len(keys), partitionToKeys)
	if err != nil {
		return nil, err
	}
	return m.convertPairsToEntries(pairs)
}

// GetEntrySet returns a clone of the mappings contained in this map.
func (m *Map) GetEntrySet(ctx context.Context) ([]types.Entry, error) {
	request := codec.EncodeMapEntrySetRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return nil, err
	} else {
		return m.convertPairsToEntries(codec.DecodeMapEntrySetResponse(response))
	}
}

// GetEntrySetWithPredicate returns a clone of the mappings contained in this map.
func (m *Map) GetEntrySetWithPredicate(ctx context.Context, predicate predicate.Predicate) ([]types.Entry, error) {
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
// If there is no entry view for the key, nil is returned.
func (m *Map) GetEntryView(ctx context.Context, key interface{}) (*types.SimpleEntryView, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapGetEntryViewRequest(m.name, keyData, lid)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			ev, maxIdle := codec.DecodeMapGetEntryViewResponse(response)
			if ev == nil {
				return nil, nil
			}
			// XXX: creating a new SimpleEntryView here in order to convert key, data and use maxIdle
			deserializedKey, err := m.convertToObject(ev.Key.(serialization.Data))
			if err != nil {
				return nil, err
			}
			deserializedValue, err := m.convertToObject(ev.Value.(serialization.Data))
			if err != nil {
				return nil, err
			}
			newEntryView := types.NewSimpleEntryView(
				deserializedKey,
				deserializedValue,
				ev.Cost,
				ev.CreationTime,
				ev.ExpirationTime,
				ev.Hits,
				ev.LastAccessTime,
				ev.LastStoredTime,
				ev.LastUpdateTime,
				ev.Version,
				ev.TTL,
				maxIdle)
			return newEntryView, nil
		}
	}
}

// GetKeySet returns keys contained in this map.
func (m *Map) GetKeySet(ctx context.Context) ([]interface{}, error) {
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

// GetKeySetWithPredicate returns keys contained in this map.
func (m *Map) GetKeySetWithPredicate(ctx context.Context, predicate predicate.Predicate) ([]interface{}, error) {
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

// GetValues returns a list clone of the values contained in this map.
func (m *Map) GetValues(ctx context.Context) ([]interface{}, error) {
	request := codec.EncodeMapValuesRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return nil, err
	} else {
		return m.convertToObjects(codec.DecodeMapValuesResponse(response))
	}
}

// GetValuesWithPredicate returns a list clone of the values contained in this map.
func (m *Map) GetValuesWithPredicate(ctx context.Context, predicate predicate.Predicate) ([]interface{}, error) {
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
func (m *Map) IsEmpty(ctx context.Context) (bool, error) {
	request := codec.EncodeMapIsEmptyRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return false, err
	} else {
		return codec.DecodeMapIsEmptyResponse(response), nil
	}
}

// IsLocked checks the lock for the specified key.
func (m *Map) IsLocked(ctx context.Context, key interface{}) (bool, error) {
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
func (m *Map) LoadAllWithoutReplacing(ctx context.Context, keys ...interface{}) error {
	return m.loadAll(ctx, false, keys...)
}

// LoadAllReplacing loads all keys from the store at server side or loads the given keys if provided.
// Replaces existing keys.
func (m *Map) LoadAllReplacing(ctx context.Context, keys ...interface{}) error {
	return m.loadAll(ctx, true, keys...)
}

/*
Lock acquires the lock for the specified key infinitely.
If the lock is not available, the current goroutine is blocked until the lock is acquired using the same lock context.

You get a lock whether the value is present in the map or not.
Other goroutines or threads on other systems would block on their invoke of Lock until the non-existent key is unlocked.
If the lock holder introduces the key to the map, the Put operation is not blocked.
If a goroutine not holding a lock on the non-existent key tries to introduce the key while a lock exists on the non-existent key, the Put operation blocks until it is unlocked.

Scope of the lock is this Map only.
Acquired lock is only for the key in this map.

Locks are re-entrant.
If the key is locked N times, it should be unlocked N times before another goroutine can acquire it.
*/
func (m *Map) Lock(ctx context.Context, key interface{}) error {
	return m.lock(ctx, key, ttlUnset)
}

// LockWithLease acquires the lock for the specified lease time.
// Otherwise, it behaves the same as Lock function.
func (m *Map) LockWithLease(ctx context.Context, key interface{}, leaseTime time.Duration) error {
	return m.lock(ctx, key, leaseTime.Milliseconds())
}

// Put sets the value for the given key and returns the old value.
func (m *Map) Put(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	return m.putWithTTL(ctx, key, value, int64(ttlUnset))
}

// PutWithTTL sets the value for the given key and returns the old value.
// Entry will expire and get evicted after the ttl.
func (m *Map) PutWithTTL(ctx context.Context, key interface{}, value interface{}, ttl time.Duration) (interface{}, error) {
	return m.putWithTTL(ctx, key, value, ttl.Milliseconds())
}

// PutWithMaxIdle sets the value for the given key and returns the old value.
// maxIdle is the maximum time in seconds for this entry to stay idle in the map.
func (m *Map) PutWithMaxIdle(ctx context.Context, key interface{}, value interface{}, maxIdle time.Duration) (interface{}, error) {
	return m.putWithMaxIdle(ctx, key, value, ttlUnset, maxIdle.Milliseconds())
}

// PutWithTTLAndMaxIdle sets the value for the given key and returns the old value.
// Entry will expire and get evicted after the ttl.
// maxIdle is the maximum time in seconds for this entry to stay idle in the map.
func (m *Map) PutWithTTLAndMaxIdle(ctx context.Context, key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) (interface{}, error) {
	return m.putWithMaxIdle(ctx, key, value, ttl.Milliseconds(), maxIdle.Milliseconds())
}

// PutAll copies all the mappings from the specified map to this map.
// No atomicity guarantees are given. In the case of a failure, some key-value tuples may get written,
// while others are not.
func (m *Map) PutAll(ctx context.Context, entries ...types.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if m.hasNearCache {
		return m.ncm.PutAll(ctx, m, entries)
	}
	return m.putAllFromRemote(ctx, entries)
}

// PutIfAbsent associates the specified key with the given value if it is not already associated.
func (m *Map) PutIfAbsent(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	return m.putIfAbsentWithTTL(ctx, key, value, ttlUnset)
}

// PutIfAbsentWithTTL associates the specified key with the given value if it is not already associated.
// Entry will expire and get evicted after the ttl.
func (m *Map) PutIfAbsentWithTTL(ctx context.Context, key interface{}, value interface{}, ttl time.Duration) (interface{}, error) {
	return m.putIfAbsentWithTTL(ctx, key, value, ttl.Milliseconds())
}

// PutIfAbsentWithTTLAndMaxIdle associates the specified key with the given value if it is not already associated.
// Entry will expire and get evicted after the ttl.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
func (m *Map) PutIfAbsentWithTTLAndMaxIdle(ctx context.Context, key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) (interface{}, error) {
	if m.hasNearCache {
		return m.ncm.PutIfAbsentWithTTLAndMaxIdle(ctx, m, key, value, ttl, maxIdle)
	}
	return m.putIfAbsentWithTTLAndMaxIdleFromRemote(ctx, key, value, ttl, maxIdle)
}

// PutTransient sets the value for the given key.
// MapStore defined at the server side will not be called.
// The TTL defined on the server-side configuration will be used.
// Max idle time defined on the server-side configuration will be used.
func (m *Map) PutTransient(ctx context.Context, key interface{}, value interface{}) error {
	return m.putTransientWithTTL(ctx, key, value, ttlUnset)
}

// PutTransientWithTTL sets the value for the given key.
// MapStore defined at the server side will not be called.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
func (m *Map) PutTransientWithTTL(ctx context.Context, key interface{}, value interface{}, ttl time.Duration) error {
	return m.putTransientWithTTL(ctx, key, value, ttl.Milliseconds())
}

// PutTransientWithMaxIdle sets the value for the given key.
// MapStore defined at the server side will not be called.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
// Set maxIdle to 0 for infinite idle time.
func (m *Map) PutTransientWithMaxIdle(ctx context.Context, key interface{}, value interface{}, maxIdle time.Duration) error {
	return m.putTransientWithTTLAndMaxIdle(ctx, key, value, ttlUnset, maxIdle.Milliseconds())
}

// PutTransientWithTTLAndMaxIdle sets the value for the given key.
// MapStore defined at the server side will not be called.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
// Set maxIdle to 0 for infinite idle time.
func (m *Map) PutTransientWithTTLAndMaxIdle(ctx context.Context, key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) error {
	return m.putTransientWithTTLAndMaxIdle(ctx, key, value, ttl.Milliseconds(), maxIdle.Milliseconds())
}

// Remove deletes the value for the given key and returns it.
func (m *Map) Remove(ctx context.Context, key interface{}) (interface{}, error) {
	if m.hasNearCache {
		return m.ncm.Remove(ctx, m, key)
	}
	return m.removeFromRemote(ctx, key)
}

// RemoveAll deletes all entries matching the given predicate.
func (m *Map) RemoveAll(ctx context.Context, predicate predicate.Predicate) error {
	if m.hasNearCache {
		return m.ncm.RemoveAll(ctx, m, predicate)
	}
	return m.removeAllFromRemote(ctx, predicate)
}

// RemoveEntryListener removes the specified entry listener.
func (m *Map) RemoveEntryListener(ctx context.Context, subscriptionID types.UUID) error {
	return m.listenerBinder.Remove(ctx, subscriptionID)
}

// RemoveListener removes the specified entry listener.
func (m *Map) RemoveListener(ctx context.Context, subscriptionID types.UUID) error {
	return m.listenerBinder.Remove(ctx, subscriptionID)
}

// RemoveInterceptor removes the interceptor.
func (m *Map) RemoveInterceptor(ctx context.Context, registrationID string) (bool, error) {
	request := codec.EncodeMapRemoveInterceptorRequest(m.name, registrationID)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return false, nil
	} else {
		return codec.DecodeMapRemoveInterceptorResponse(response), nil
	}
}

// RemoveIfSame removes the entry for a key only if it is currently mapped to a given value.
// Returns true if the entry was removed.
func (m *Map) RemoveIfSame(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	if m.hasNearCache {
		return m.ncm.RemoveIfSame(ctx, m, key, value)
	}
	return m.removeIfSameFromRemote(ctx, key, value)
}

// Replace replaces the entry for a key only if it is currently mapped to some value and returns the previous value.
func (m *Map) Replace(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	if m.hasNearCache {
		return m.ncm.Replace(ctx, m, key, value)
	}
	return m.replaceFromRemote(ctx, key, value)
}

// ReplaceIfSame replaces the entry for a key only if it is currently mapped to a given value.
// Returns true if the value was replaced.
func (m *Map) ReplaceIfSame(ctx context.Context, key interface{}, oldValue interface{}, newValue interface{}) (bool, error) {
	if m.hasNearCache {
		return m.ncm.ReplaceIfSame(ctx, m, key, oldValue, newValue)
	}
	return m.replaceIfSameFromRemote(ctx, key, oldValue, newValue)
}

// Set sets the value for the given key.
func (m *Map) Set(ctx context.Context, key interface{}, value interface{}) error {
	return m.set(ctx, key, value, ttlUnset)
}

// SetTTL updates the TTL value of the entry specified by the given key with a new TTL value.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
func (m *Map) SetTTL(ctx context.Context, key interface{}, ttl time.Duration) error {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		request := codec.EncodeMapSetTtlRequest(m.name, keyData, ttl.Milliseconds())
		_, err := m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

// SetTTLAffected updates the TTL value of the entry specified by the given key with a new TTL value.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Returns true if entry is affected.
// Set ttl to 0 for infinite timeout.
func (m *Map) SetTTLAffected(ctx context.Context, key interface{}, ttl time.Duration) (bool, error) {
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := codec.EncodeMapSetTtlRequest(m.name, keyData, ttl.Milliseconds())
	resp, err := m.invokeOnKey(ctx, request, keyData)
	if err != nil {
		return false, err
	}
	return codec.DecodeMapSetTtlResponse(resp), nil
}

// SetWithTTL sets the value for the given key.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
func (m *Map) SetWithTTL(ctx context.Context, key interface{}, value interface{}, ttl time.Duration) error {
	return m.set(ctx, key, value, ttl.Milliseconds())
}

// SetWithTTLAndMaxIdle sets the value for the given key.
// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
// Set ttl to 0 for infinite timeout.
// Given max idle time (maximum time for this entry to stay idle in the map) is used.
// Set maxIdle to 0 for infinite idle time.
func (m *Map) SetWithTTLAndMaxIdle(ctx context.Context, key, value interface{}, ttl time.Duration, maxIdle time.Duration) error {
	if m.hasNearCache {
		return m.ncm.SetWithTTLAndMaxIdle(ctx, m, key, value, ttl, maxIdle)
	}
	return m.setWithTTLAndMaxIdleFromRemote(ctx, key, value, ttl, maxIdle)
}

// Size returns the number of entries in this map.
func (m *Map) Size(ctx context.Context) (int, error) {
	request := codec.EncodeMapSizeRequest(m.name)
	if response, err := m.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeMapSizeResponse(response)), nil
	}
}

// TryLock tries to acquire the lock for the specified key.
// When the lock is not available, the current goroutine doesn't wait and returns false immediately.
func (m *Map) TryLock(ctx context.Context, key interface{}) (bool, error) {
	return m.tryLock(ctx, key, leaseUnset, 0)
}

// TryLockWithLease tries to acquire the lock for the specified key.
// Lock will be released after lease time passes.
func (m *Map) TryLockWithLease(ctx context.Context, key interface{}, lease time.Duration) (bool, error) {
	return m.tryLock(ctx, key, lease.Milliseconds(), 0)
}

// TryLockWithTimeout tries to acquire the lock for the specified key.
// The current goroutine is blocked until the lock is acquired using the same lock context, or he specified waiting time elapses.
func (m *Map) TryLockWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (bool, error) {
	return m.tryLock(ctx, key, leaseUnset, timeout.Milliseconds())
}

// TryLockWithLeaseAndTimeout tries to acquire the lock for the specified key.
// The current goroutine is blocked until the lock is acquired using the same lock context, or he specified waiting time elapses.
// Lock will be released after lease time passes.
func (m *Map) TryLockWithLeaseAndTimeout(ctx context.Context, key interface{}, lease time.Duration, timeout time.Duration) (bool, error) {
	return m.tryLock(ctx, key, lease.Milliseconds(), timeout.Milliseconds())
}

// TryPut tries to put the given key and value into this map and returns immediately.
func (m *Map) TryPut(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	return m.tryPut(ctx, key, value, 0)
}

// TryPutWithTimeout tries to put the given key and value into this map and waits until operation is completed or the given timeout is reached.
func (m *Map) TryPutWithTimeout(ctx context.Context, key interface{}, value interface{}, timeout time.Duration) (bool, error) {
	return m.tryPut(ctx, key, value, timeout.Milliseconds())
}

// TryRemove tries to remove the given key from this map and returns immediately.
func (m *Map) TryRemove(ctx context.Context, key interface{}) (interface{}, error) {
	return m.tryRemove(ctx, key, 0)
}

// TryRemoveWithTimeout tries to remove the given key from this map and waits until operation is completed or timeout is reached.
func (m *Map) TryRemoveWithTimeout(ctx context.Context, key interface{}, timeout time.Duration) (interface{}, error) {
	return m.tryRemove(ctx, key, timeout.Milliseconds())
}

// Unlock releases the lock for the specified key.
func (m *Map) Unlock(ctx context.Context, key interface{}) error {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.refIDGen.NextID()
		request := codec.EncodeMapUnlockRequest(m.name, keyData, lid, refID)
		_, err = m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

func (m *Map) LocalMapStats() LocalMapStats {
	if m.hasNearCache {
		return m.ncm.GetLocalMapStats()
	}
	return LocalMapStats{}
}

func (m *Map) destroyLocally(ctx context.Context) bool {
	m.logger.Trace(func() string {
		return fmt.Sprintf("hazelcast.Map.destroyLocally: %s", m.name)
	})
	if m.hasNearCache {
		if err := m.ncm.Destroy(ctx, m.name); err != nil {
			m.logger.Errorf("hazelcast.Map.destroyLocally: %w", err)
		}
	}
	return true
}

func (m *Map) addIndex(ctx context.Context, indexConfig types.IndexConfig) error {
	if err := validateAndNormalizeIndexConfig(&indexConfig); err != nil {
		return err
	}
	request := codec.EncodeMapAddIndexRequest(m.name, indexConfig)
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

func (m *Map) addEntryListener(ctx context.Context, flags int32, includeValue bool, key interface{}, predicate predicate.Predicate, handler EntryNotifiedHandler) (types.UUID, error) {
	var err error
	var keyData serialization.Data
	var predicateData serialization.Data
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
	addRequest := m.makeListenerRequest(keyData, predicateData, flags, includeValue)
	listenerHandler := func(msg *proto.ClientMessage) {
		m.makeListenerDecoder(msg, keyData, predicateData, m.makeEntryNotifiedListenerHandler(handler))
	}
	removeRequest := codec.EncodeMapRemoveEntryListenerRequest(m.name, subscriptionID)
	err = m.listenerBinder.Add(ctx, subscriptionID, addRequest, removeRequest, listenerHandler)
	return subscriptionID, err
}

func (m *Map) loadAll(ctx context.Context, replaceExisting bool, keys ...interface{}) error {
	if len(keys) == 0 {
		return nil
	}
	if m.hasNearCache {
		return m.ncm.LoadAll(ctx, m, replaceExisting, keys)
	}
	return m.loadAllFromRemote(ctx, replaceExisting, keys)
}

func (m *Map) convertToDataList(keys []interface{}) ([]serialization.Data, error) {
	keyDatas := make([]serialization.Data, 0, len(keys))
	for _, key := range keys {
		keyData, err := m.validateAndSerialize(key)
		if err != nil {
			return nil, err
		}
		keyDatas = append(keyDatas, keyData)
	}
	return keyDatas, nil
}

func (m *Map) lock(ctx context.Context, key interface{}, ttl int64) error {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.refIDGen.NextID()
		request := codec.EncodeMapLockRequest(m.name, keyData, lid, ttl, refID)
		_, err = m.invokeOnKey(ctx, request, keyData)
		return err
	}
}

func (m *Map) putWithTTL(ctx context.Context, key, value interface{}, ttl int64) (interface{}, error) {
	if m.hasNearCache {
		return m.ncm.Put(ctx, m, key, value, ttl)
	}
	return m.putWithTTLFromRemote(ctx, key, value, ttl)
}

func (m *Map) putWithMaxIdle(ctx context.Context, key, value interface{}, ttl int64, maxIdle int64) (interface{}, error) {
	if m.hasNearCache {
		return m.ncm.PutWithMaxIdle(ctx, m, key, value, ttl, maxIdle)
	}
	return m.putWithMaxIdleFromRemote(ctx, key, value, ttl, maxIdle)
}

func (m *Map) putIfAbsentWithTTL(ctx context.Context, key interface{}, value interface{}, ttl int64) (interface{}, error) {
	if m.hasNearCache {
		return m.ncm.PutIfAbsentWithTTL(ctx, m, key, value, ttl)
	}
	return m.putIfAbsentWithTTLFromRemote(ctx, key, value, ttl)
}

func (m *Map) putTransientWithTTL(ctx context.Context, key interface{}, value interface{}, ttl int64) error {
	if m.hasNearCache {
		return m.ncm.PutTransientWithTTL(ctx, m, key, value, ttl)
	}
	return m.putTransientWithTTLFromRemote(ctx, key, value, ttl)
}

func (m *Map) putTransientWithTTLAndMaxIdle(ctx context.Context, key interface{}, value interface{}, ttl int64, maxIdle int64) error {
	if m.hasNearCache {
		return m.ncm.PutTransientWithTTLAndMaxIdle(ctx, m, key, value, ttl, maxIdle)
	}
	return m.putTransientWithTTLAndMaxIdleFromRemote(ctx, key, value, ttl, maxIdle)
}

func (m *Map) tryLock(ctx context.Context, key interface{}, lease int64, timeout int64) (bool, error) {
	lid := extractLockID(ctx)
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		refID := m.refIDGen.NextID()
		request := codec.EncodeMapTryLockRequest(m.name, keyData, lid, lease, timeout, refID)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapTryLockResponse(response), nil
		}
	}
}

func (m *Map) makeListenerRequest(keyData, predicateData serialization.Data, flags int32, includeValue bool) *proto.ClientMessage {
	if keyData != nil {
		if predicateData != nil {
			return codec.EncodeMapAddEntryListenerToKeyWithPredicateRequest(m.name, keyData, predicateData, includeValue, flags, m.smart)
		}
		return codec.EncodeMapAddEntryListenerToKeyRequest(m.name, keyData, includeValue, flags, m.smart)
	}
	if predicateData != nil {
		return codec.EncodeMapAddEntryListenerWithPredicateRequest(m.name, predicateData, includeValue, flags, m.smart)
	}
	return codec.EncodeMapAddEntryListenerRequest(m.name, includeValue, flags, m.smart)
}

func (m *Map) makeListenerDecoder(msg *proto.ClientMessage, keyData, predicateData serialization.Data, handler entryNotifiedHandler) {
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

func (m *Map) set(ctx context.Context, key, value interface{}, ttl int64) error {
	if m.hasNearCache {
		return m.ncm.Set(ctx, m, key, value, ttl)
	}
	return m.setFromRemote(ctx, key, value, ttl)
}

func (m *Map) tryPut(ctx context.Context, key interface{}, value interface{}, timeout int64) (bool, error) {
	if m.hasNearCache {
		return m.ncm.TryPut(ctx, m, key, value, timeout)
	}
	return m.tryPutFromRemote(ctx, key, value, timeout)
}

func (m *Map) tryRemove(ctx context.Context, key interface{}, timeout int64) (interface{}, error) {
	if m.hasNearCache {
		return m.ncm.TryRemove(ctx, m, key, timeout)
	}
	return m.tryRemoveFromRemote(ctx, key, timeout)
}

func (m *Map) aggregate(ctx context.Context, req *proto.ClientMessage, decoder func(message *proto.ClientMessage) serialization.Data) (interface{}, error) {
	resp, err := m.invokeOnRandomTarget(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	data := decoder(resp)
	obj, err := m.convertToObject(data)
	if err != nil {
		return nil, err
	}
	// if this is a canonicalizing dereference it
	cs, ok := obj.(*iproxy.AggCanonicalizingSet)
	if ok {
		return *cs, nil
	}
	return obj, nil
}

func (m *Map) partitionToKeys(keys []interface{}, serializedKeys bool) (map[int32][]serialization.Data, error) {
	res := map[int32][]serialization.Data{}
	ps := m.proxy.partitionService
	var err error
	for _, key := range keys {
		var keyData serialization.Data
		if serializedKeys {
			keyData = key.(serialization.Data)
		} else {
			keyData, err = m.validateAndSerialize(key)
			if err != nil {
				return nil, err
			}
		}
		pk, err := ps.GetPartitionID(keyData)
		if err != nil {
			return nil, err
		}
		arr := res[pk]
		res[pk] = append(arr, keyData)
	}
	return res, nil
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
		return ihzerrors.NewIllegalArgumentError("index must have at least one attribute", nil)
	}
	if len(attrs) > maxIndexAttributes {
		return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("index cannot have more than %d attributes", maxIndexAttributes), nil)
	}
	if ic.Type == types.IndexTypeBitmap && len(attrs) > 1 {
		return ihzerrors.NewIllegalArgumentError("composite bitmap indexes are not supported", nil)
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
		return ihzerrors.NewIllegalArgumentError("attribute name cannot be not empty", nil)
	}
	if strings.HasSuffix(attr, ".") {
		return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("attribute name cannot end with dot: %s", attr), nil)
	}
	if strings.HasPrefix(attr, "this.") {
		attr = strings.Replace(attr, "this.", "", 1)
		if attr == "" {
			return ihzerrors.NewIllegalArgumentError("attribute name cannot be 'this.'", nil)
		}
	}
	if _, ok := as.attrs[attr]; ok {
		return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("duplicate attribute name not allowed: %s", attr), nil)
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

// MapEntryListenerConfig contains configuration for a map entry listener.
type MapEntryListenerConfig struct {
	Predicate    predicate.Predicate
	Key          interface{}
	flags        int32
	IncludeValue bool
}

// NotifyEntryAdded enables receiving an entry event when an entry is added.
// Deprecated: See AddEntryListener's deprecation notice.
func (c *MapEntryListenerConfig) NotifyEntryAdded(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryAdded), enable)
}

// NotifyEntryRemoved enables receiving an entry event when an entry is removed.
// Deprecated: See AddEntryListener's deprecation notice.
func (c *MapEntryListenerConfig) NotifyEntryRemoved(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryRemoved), enable)
}

// NotifyEntryUpdated enables receiving an entry event when an entry is updated.
// Deprecated: See AddEntryListener's deprecation notice.
func (c *MapEntryListenerConfig) NotifyEntryUpdated(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryUpdated), enable)
}

// NotifyEntryEvicted enables receiving an entry event when an entry is evicted.
// Deprecated: See AddEntryListener's deprecation notice.
func (c *MapEntryListenerConfig) NotifyEntryEvicted(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryEvicted), enable)
}

// NotifyEntryExpired enables receiving an entry event when an entry is expired.
// Deprecated: See AddEntryListener's deprecation notice.
func (c *MapEntryListenerConfig) NotifyEntryExpired(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryExpired), enable)
}

// NotifyEntryAllEvicted enables receiving an entry event when all entries are evicted.
// Deprecated: See AddEntryListener's deprecation notice.
func (c *MapEntryListenerConfig) NotifyEntryAllEvicted(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryAllEvicted), enable)
}

// NotifyEntryAllCleared enables receiving an entry event when all entries are cleared.
// Deprecated: See AddEntryListener's deprecation notice.
func (c *MapEntryListenerConfig) NotifyEntryAllCleared(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryAllCleared), enable)
}

// NotifyEntryMerged enables receiving an entry event when an entry is merged.
// Deprecated: See AddEntryListener's deprecation notice.
func (c *MapEntryListenerConfig) NotifyEntryMerged(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryMerged), enable)
}

// NotifyEntryInvalidated enables receiving an entry event when an entry is invalidated.
// Deprecated: See AddEntryListener's deprecation notice.
func (c *MapEntryListenerConfig) NotifyEntryInvalidated(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryInvalidated), enable)
}

// NotifyEntryLoaded enables receiving an entry event when an entry is loaded.
// Deprecated: See AddEntryListener's deprecation notice.
func (c *MapEntryListenerConfig) NotifyEntryLoaded(enable bool) {
	flagsSetOrClear(&c.flags, int32(EntryLoaded), enable)
}

type LocalMapStats struct {
	NearCacheStats nearcache.Stats
}
