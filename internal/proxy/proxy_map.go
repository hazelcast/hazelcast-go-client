// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"time"

	"github.com/hazelcast/hazelcast-go-client/hztypes"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

type MapImpl struct {
	*Proxy
	referenceIDGenerator ReferenceIDGenerator
}

func NewMapImpl(proxy *Proxy) *MapImpl {
	return &MapImpl{
		Proxy:                proxy,
		referenceIDGenerator: NewReferenceIDGeneratorImpl(),
	}
}

func (m *MapImpl) AddIndex(indexConfig hztypes.IndexConfig) error {
	request := codec.EncodeMapAddIndexRequest(m.name, indexConfig)
	_, err := m.invokeOnRandomTarget(request, nil)
	return err
}

func (m *MapImpl) AddInterceptor(interceptor interface{}) (string, error) {
	if interceptorData, err := m.Proxy.convertToData(interceptor); err != nil {
		return "", err
	} else {
		request := codec.EncodeMapAddInterceptorRequest(m.name, interceptorData)
		if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
			return "", err
		} else {
			return codec.DecodeMapAddInterceptorResponse(response), nil
		}
	}
}

func (m *MapImpl) Clear() error {
	request := codec.EncodeMapClearRequest(m.name)
	_, err := m.invokeOnRandomTarget(request, nil)
	return err
}

func (m *MapImpl) ContainsKey(key interface{}) (bool, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapContainsKeyRequest(m.name, keyData, threadID())
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapContainsKeyResponse(response), nil
		}
	}
}

func (m *MapImpl) ContainsValue(value interface{}) (bool, error) {
	if valueData, err := m.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapContainsValueRequest(m.name, valueData)
		if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
			return false, err
		} else {
			return codec.DecodeMapContainsValueResponse(response), nil
		}
	}
}

func (m *MapImpl) Delete(key interface{}) error {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		request := codec.EncodeMapDeleteRequest(m.name, keyData, threadID())
		_, err := m.invokeOnKey(request, keyData)
		return err
	}
}

func (m *MapImpl) Evict(key interface{}) (bool, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapEvictRequest(m.name, keyData, threadID())
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapEvictResponse(response), nil
		}
	}
}

func (m *MapImpl) EvictAll() error {
	request := codec.EncodeMapEvictAllRequest(m.name)
	_, err := m.invokeOnRandomTarget(request, nil)
	return err
}

func (m *MapImpl) ExecuteOnEntries(entryProcessor interface{}) ([]hztypes.Entry, error) {
	if processorData, err := m.validateAndSerialize(entryProcessor); err != nil {
		return nil, err
	} else {
		ch := make(chan []proto.Pair, 1)
		handler := func(msg *proto.ClientMessage) {
			ch <- codec.DecodeMapExecuteOnAllKeysResponse(msg)
		}
		request := codec.EncodeMapExecuteOnAllKeysRequest(m.name, processorData)
		if _, err := m.invokeOnRandomTarget(request, handler); err != nil {
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

func (m *MapImpl) Flush() error {
	request := codec.EncodeMapFlushRequest(m.name)
	_, err := m.invokeOnRandomTarget(request, nil)
	return err
}

func (m *MapImpl) ForceUnlock(key interface{}) error {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.referenceIDGenerator.NextID()
		request := codec.EncodeMapForceUnlockRequest(m.name, keyData, refID)
		_, err = m.invokeOnKey(request, keyData)
		return err
	}
}

func (m *MapImpl) Get(key interface{}) (interface{}, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapGetRequest(m.name, keyData, threadID())
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeMapGetResponse(response))
		}
	}
}

func (m *MapImpl) GetAll(keys ...interface{}) ([]hztypes.Entry, error) {
	partitionToKeys := map[int32][]pubserialization.Data{}
	ps := m.Proxy.partitionService
	for _, key := range keys {
		if keyData, err := m.validateAndSerialize(key); err != nil {
			return nil, err
		} else {
			partitionKey := ps.GetPartitionID(keyData)
			arr := partitionToKeys[partitionKey]
			partitionToKeys[partitionKey] = append(arr, keyData)
		}
	}
	result := make([]hztypes.Entry, 0, len(keys))
	// create invocations
	invs := make([]invocation.Invocation, 0, len(partitionToKeys))
	for partitionID, keys := range partitionToKeys {
		request := codec.EncodeMapGetAllRequest(m.name, keys)
		inv := m.invokeOnPartitionAsync(request, partitionID)
		invs = append(invs, inv)
	}
	// wait for responses and decode them
	for _, inv := range invs {
		if response, err := inv.Get(); err != nil {
			// TODO: prevent leak when some inv.Get()s are not executed due to error of other ones.
			return nil, err
		} else {
			pairs := codec.DecodeMapGetAllResponse(response)
			var key, value interface{}
			var err error
			for _, pair := range pairs {
				if key, err = m.convertToObject(pair.Key().(pubserialization.Data)); err != nil {
					return nil, err
				} else if value, err = m.convertToObject(pair.Value().(pubserialization.Data)); err != nil {
					return nil, err
				}
				result = append(result, hztypes.NewEntry(key, value))
			}
		}
	}
	return result, nil
}

func (m *MapImpl) GetEntrySet() ([]hztypes.Entry, error) {
	request := codec.EncodeMapEntrySetRequest(m.name)
	if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
		return nil, err
	} else {
		return m.convertPairsToEntries(codec.DecodeMapEntrySetResponse(response))
	}
}

func (m *MapImpl) GetEntrySetWithPredicate(predicate predicate.Predicate) ([]hztypes.Entry, error) {
	if predData, err := m.validateAndSerialize(predicate); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapEntriesWithPredicateRequest(m.name, predData)
		if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
			return nil, err
		} else {
			return m.convertPairsToEntries(codec.DecodeMapEntriesWithPredicateResponse(response))
		}
	}
}

func (m *MapImpl) GetEntryView(key string) (*hztypes.SimpleEntryView, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapGetEntryViewRequest(m.name, keyData, threadID())
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			ev, maxIdle := codec.DecodeMapGetEntryViewResponse(response)
			// XXX: creating a new SimpleEntryView here in order to convert key, data and use maxIdle
			deserializedKey, err := m.convertToObject(ev.Key().(pubserialization.Data))
			if err != nil {
				return nil, err
			}
			deserializedValue, err := m.convertToObject(ev.Value().(pubserialization.Data))
			if err != nil {
				return nil, err
			}
			newEntryView := hztypes.NewSimpleEntryView(
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

func (m *MapImpl) GetKeySet() ([]interface{}, error) {
	request := codec.EncodeMapKeySetRequest(m.name)
	if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
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

func (m *MapImpl) GetKeySetWithPredicate(predicate predicate.Predicate) ([]interface{}, error) {
	if predicateData, err := m.validateAndSerializePredicate(predicate); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapKeySetWithPredicateRequest(m.name, predicateData)
		if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
			return nil, err
		} else {
			return m.convertToObjects(codec.DecodeMapKeySetWithPredicateResponse(response))
		}
	}
}

func (m *MapImpl) GetValues() ([]interface{}, error) {
	request := codec.EncodeMapValuesRequest(m.name)
	if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
		return nil, err
	} else {
		return m.convertToObjects(codec.DecodeMapValuesResponse(response))
	}
}

func (m *MapImpl) GetValuesWithPredicate(predicate predicate.Predicate) ([]interface{}, error) {
	if predicateData, err := m.validateAndSerializePredicate(predicate); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapValuesWithPredicateRequest(m.name, predicateData)
		if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
			return nil, err
		} else {
			return m.convertToObjects(codec.DecodeMapValuesWithPredicateResponse(response))
		}
	}
}

func (m *MapImpl) IsEmpty() (bool, error) {
	request := codec.EncodeMapIsEmptyRequest(m.name)
	if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
		return false, err
	} else {
		return codec.DecodeMapIsEmptyResponse(response), nil
	}
}

func (m *MapImpl) IsLocked(key interface{}) (bool, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapIsLockedRequest(m.name, keyData)
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapIsLockedResponse(response), nil
		}
	}
}

func (m *MapImpl) LoadAll(keys ...interface{}) error {
	return m.loadAll(false, keys...)
}

func (m *MapImpl) LoadAllReplacingExisting(keys ...interface{}) error {
	return m.loadAll(true, keys...)
}

func (m *MapImpl) Lock(key interface{}) error {
	return m.lock(key, ttlDefault)
}

func (m *MapImpl) LockWithLease(key interface{}, leaseTime time.Duration) error {
	return m.lock(key, leaseTime.Milliseconds())
}

func (m *MapImpl) Put(key interface{}, value interface{}) (interface{}, error) {
	return m.putTTL(key, value, ttlDefault)
}

func (m *MapImpl) PutWithTTL(key interface{}, value interface{}, ttl time.Duration) (interface{}, error) {
	return m.putTTL(key, value, ttl.Milliseconds())
}

func (m *MapImpl) PutWithMaxIdle(key interface{}, value interface{}, maxIdle time.Duration) (interface{}, error) {
	return m.putMaxIdle(key, value, ttlDefault, maxIdle.Milliseconds())
}

func (m *MapImpl) PutWithTTLAndMaxIdle(key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) (interface{}, error) {
	return m.putMaxIdle(key, value, ttl.Milliseconds(), maxIdle.Milliseconds())
}

func (m *MapImpl) PutAll(keyValuePairs []hztypes.Entry) error {
	if partitionToPairs, err := m.partitionToPairs(keyValuePairs); err != nil {
		return err
	} else {
		// create invocations
		invs := make([]invocation.Invocation, 0, len(partitionToPairs))
		for partitionID, entries := range partitionToPairs {
			inv := m.invokeOnPartitionAsync(codec.EncodeMapPutAllRequest(m.name, entries, true), partitionID)
			invs = append(invs, inv)
		}
		// wait for responses
		for _, inv := range invs {
			if _, err := inv.Get(); err != nil {
				// TODO: prevent leak when some inv.Get()s are not executed due to error of other ones.
				return err
			}
		}
		return nil
	}
}

func (m *MapImpl) PutIfAbsent(key interface{}, value interface{}) (interface{}, error) {
	return m.putIfAbsent(key, value, ttlDefault)
}

func (m *MapImpl) PutIfAbsentWithTTL(key interface{}, value interface{}, ttl time.Duration) (interface{}, error) {
	return m.putIfAbsent(key, value, ttl.Milliseconds())
}

func (m *MapImpl) PutIfAbsentWithTTLAndMaxIdle(key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) (interface{}, error) {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapPutIfAbsentWithMaxIdleRequest(m.name, keyData, valueData, threadID(), ttl.Milliseconds(), maxIdle.Milliseconds())
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return codec.DecodeMapPutIfAbsentWithMaxIdleResponse(response), nil
		}
	}
}

func (m *MapImpl) PutTransient(key interface{}, value interface{}) error {
	return m.putTransient(key, value, ttlDefault, maxIdleDefault)
}

func (m *MapImpl) PutTransientWithTTL(key interface{}, value interface{}, ttl time.Duration) error {
	return m.putTransient(key, value, ttl.Milliseconds(), maxIdleDefault)
}

func (m *MapImpl) PutTransientWithMaxIdle(key interface{}, value interface{}, maxIdle time.Duration) error {
	return m.putTransient(key, value, ttlDefault, maxIdle.Milliseconds())
}

func (m *MapImpl) PutTransientWithTTLMaxIdle(key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) error {
	return m.putTransient(key, value, ttl.Milliseconds(), maxIdle.Milliseconds())
}

func (m *MapImpl) Remove(key interface{}) (interface{}, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapRemoveRequest(m.name, keyData, threadID())
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeMapRemoveResponse(response))
		}
	}
}

func (m *MapImpl) RemoveAll(predicate predicate.Predicate) error {
	if predicateData, err := m.validateAndSerialize(predicate); err != nil {
		return err
	} else {
		request := codec.EncodeMapRemoveAllRequest(m.name, predicateData)
		_, err := m.invokeOnRandomTarget(request, nil)
		return err
	}
}

func (m *MapImpl) RemoveInterceptor(registrationID string) (bool, error) {
	request := codec.EncodeMapRemoveInterceptorRequest(m.name, registrationID)
	if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
		return false, nil
	} else {
		return codec.DecodeMapRemoveInterceptorResponse(response), nil
	}
}

func (m *MapImpl) RemoveIfSame(key interface{}, value interface{}) (bool, error) {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapRemoveIfSameRequest(m.name, keyData, valueData, threadID())
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapRemoveIfSameResponse(response), nil
		}
	}
}

func (m *MapImpl) Replace(key interface{}, value interface{}) (interface{}, error) {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapReplaceRequest(m.name, keyData, valueData, threadID())
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeMapReplaceResponse(response))
		}
	}
}

func (m *MapImpl) ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (bool, error) {
	if keyData, oldValueData, newValueData, err := m.validateAndSerialize3(key, oldValue, newValue); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapReplaceIfSameRequest(m.name, keyData, oldValueData, newValueData, threadID())
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapReplaceIfSameResponse(response), nil
		}
	}
}

func (m *MapImpl) Set(key interface{}, value interface{}) error {
	return m.set(key, value, ttlDefault)
}

func (m *MapImpl) SetTTL(key interface{}, ttl time.Duration) error {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		request := codec.EncodeMapSetTtlRequest(m.name, keyData, ttl.Milliseconds())
		_, err := m.invokeOnKey(request, keyData)
		return err
	}
}

func (m *MapImpl) SetWithTTL(key interface{}, value interface{}, ttl time.Duration) error {
	return m.set(key, value, ttl.Milliseconds())
}

func (m *MapImpl) SetWithTTLAndMaxIdle(key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) error {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return err
	} else {
		request := codec.EncodeMapSetWithMaxIdleRequest(m.name, keyData, valueData, threadID(), ttl.Milliseconds(), maxIdle.Milliseconds())
		_, err := m.invokeOnKey(request, keyData)
		return err
	}
}

func (m *MapImpl) Size() (int, error) {
	request := codec.EncodeMapSizeRequest(m.name)
	if response, err := m.invokeOnRandomTarget(request, nil); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeMapSizeResponse(response)), nil
	}
}

func (m *MapImpl) TryLock(key interface{}) (bool, error) {
	return m.tryLock(key, 0, 0)
}

func (m *MapImpl) TryLockWithLease(key interface{}, lease time.Duration) (bool, error) {
	return m.tryLock(key, lease.Milliseconds(), 0)
}

func (m *MapImpl) TryLockWithTimeout(key interface{}, timeout time.Duration) (bool, error) {
	return m.tryLock(key, 0, timeout.Milliseconds())
}

func (m *MapImpl) TryLockWithLeaseTimeout(key interface{}, lease time.Duration, timeout time.Duration) (bool, error) {
	return m.tryLock(key, lease.Milliseconds(), timeout.Milliseconds())
}

func (m *MapImpl) TryPut(key interface{}, value interface{}) (interface{}, error) {
	return m.tryPut(key, value, 0)
}

func (m *MapImpl) TryPutWithTimeout(key interface{}, value interface{}, timeout time.Duration) (interface{}, error) {
	return m.tryPut(key, value, timeout.Milliseconds())
}

func (m *MapImpl) TryRemove(key interface{}) (interface{}, error) {
	return m.tryRemove(key, 0)
}

func (m *MapImpl) TryRemoveWithTimeout(key interface{}, timeout time.Duration) (interface{}, error) {
	return m.tryRemove(key, timeout.Milliseconds())
}

func (m *MapImpl) Unlock(key interface{}) error {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.referenceIDGenerator.NextID()
		request := codec.EncodeMapUnlockRequest(m.name, keyData, threadID(), refID)
		_, err = m.invokeOnKey(request, keyData)
		return err
	}
}

func (m *MapImpl) ListenEntryNotification(config hztypes.MapEntryListenerConfig, subscriptionID int, handler hztypes.EntryNotifiedHandler) error {
	flags := makeListenerFlags(&config)
	return m.listenEntryNotified(flags, config.IncludeValue, config.Key, config.Predicate, subscriptionID, handler)
}

func (m *MapImpl) UnlistenEntryNotification(subscriptionID int) error {
	m.userEventDispatcher.Unsubscribe(hztypes.EventEntryNotified, subscriptionID)
	return m.listenerBinder.Remove(m.name, subscriptionID)
}

func (m *MapImpl) listenEntryNotified(flags int32, includeValue bool, key interface{}, predicate predicate.Predicate, subscriptionID int, handler hztypes.EntryNotifiedHandler) error {
	var request *proto.ClientMessage
	var err error
	var keyData pubserialization.Data
	var predicateData pubserialization.Data
	if key != nil {
		if keyData, err = m.validateAndSerialize(key); err != nil {
			return err
		}
	}
	if predicate != nil {
		if predicateData, err = m.validateAndSerialize(predicate); err != nil {
			return err
		}
	}
	if keyData != nil {
		if predicateData != nil {
			request = codec.EncodeMapAddEntryListenerToKeyWithPredicateRequest(m.name, keyData, predicateData, includeValue, flags, m.smartRouting)
		} else {
			request = codec.EncodeMapAddEntryListenerToKeyRequest(m.name, keyData, includeValue, flags, m.smartRouting)
		}
	} else if predicateData != nil {
		request = codec.EncodeMapAddEntryListenerWithPredicateRequest(m.name, predicateData, includeValue, flags, m.smartRouting)
	} else {
		request = codec.EncodeMapAddEntryListenerRequest(m.name, includeValue, flags, m.smartRouting)
	}
	err = m.listenerBinder.Add(request, subscriptionID, func(msg *proto.ClientMessage) {
		handler := func(binKey pubserialization.Data, binValue pubserialization.Data, binOldValue pubserialization.Data, binMergingValue pubserialization.Data, binEventType int32, binUUID internal.UUID, numberOfAffectedEntries int32) {
			key := m.mustConvertToInterface(binKey, "invalid key at ListenEntryNotification")
			value := m.mustConvertToInterface(binValue, "invalid value at ListenEntryNotification")
			oldValue := m.mustConvertToInterface(binOldValue, "invalid oldValue at ListenEntryNotification")
			mergingValue := m.mustConvertToInterface(binMergingValue, "invalid mergingValue at ListenEntryNotification")
			m.userEventDispatcher.Publish(newEntryNotifiedEventImpl(m.name, binUUID.String(), key, value, oldValue, mergingValue, int(numberOfAffectedEntries)))
		}
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
	})
	if err != nil {
		return err
	}
	m.userEventDispatcher.Subscribe(hztypes.EventEntryNotified, subscriptionID, func(event event.Event) {
		if entryNotifiedEvent, ok := event.(*hztypes.EntryNotified); ok {
			if entryNotifiedEvent.OwnerName == m.name {
				handler(entryNotifiedEvent)
			}
		} else {
			panic("cannot cast event to hztypes.EntryNotified event")
		}
	})
	return nil
}

func (m *MapImpl) loadAll(replaceExisting bool, keys ...interface{}) error {
	if len(keys) == 0 {
		return nil
	}
	keyDatas := make([]pubserialization.Data, 0, len(keys))
	for _, key := range keys {
		if keyData, err := m.convertToData(key); err != nil {
			return err
		} else {
			keyDatas = append(keyDatas, keyData)
		}
	}
	request := codec.EncodeMapLoadGivenKeysRequest(m.name, keyDatas, replaceExisting)
	_, err := m.invokeOnRandomTarget(request, nil)
	return err
}

func (m *MapImpl) lock(key interface{}, ttl int64) error {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return err
	} else {
		refID := m.referenceIDGenerator.NextID()
		request := codec.EncodeMapLockRequest(m.name, keyData, threadID(), ttl, refID)
		_, err = m.invokeOnKey(request, keyData)
		return err
	}
}

func (m *MapImpl) putTTL(key interface{}, value interface{}, ttl int64) (interface{}, error) {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapPutRequest(m.name, keyData, valueData, threadID(), ttl)
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeMapPutResponse(response))
		}
	}
}
func (m *MapImpl) putMaxIdle(key interface{}, value interface{}, ttl int64, maxIdle int64) (interface{}, error) {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapPutWithMaxIdleRequest(m.name, keyData, valueData, threadID(), ttl, maxIdle)
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeMapPutWithMaxIdleResponse(response))
		}
	}
}

func (m *MapImpl) putIfAbsent(key interface{}, value interface{}, ttl int64) (interface{}, error) {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapPutIfAbsentRequest(m.name, keyData, valueData, threadID(), ttl)
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return codec.DecodeMapPutIfAbsentResponse(response), nil
		}
	}
}

func (m *MapImpl) putTransient(key interface{}, value interface{}, ttl int64, maxIdle int64) error {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return err
	} else {
		var request *proto.ClientMessage
		if maxIdle >= 0 {
			request = codec.EncodeMapPutTransientWithMaxIdleRequest(m.name, keyData, valueData, threadID(), ttl, maxIdle)
		} else {
			request = codec.EncodeMapPutTransientRequest(m.name, keyData, valueData, threadID(), ttl)
		}
		_, err = m.invokeOnKey(request, keyData)
		return err
	}
}

func (m *MapImpl) set(key interface{}, value interface{}, ttl int64) error {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return err
	} else {
		request := codec.EncodeMapSetRequest(m.name, keyData, valueData, threadID(), ttl)
		_, err := m.invokeOnKey(request, keyData)
		return err
	}
}

func (m *MapImpl) tryLock(key interface{}, lease int64, timeout int64) (bool, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		refID := m.referenceIDGenerator.NextID()
		request := codec.EncodeMapTryLockRequest(m.name, keyData, threadID(), lease, timeout, refID)
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeMapTryLockResponse(response), nil
		}
	}
}

func (m *MapImpl) tryPut(key interface{}, value interface{}, timeout int64) (interface{}, error) {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeMapTryPutRequest(m.name, keyData, valueData, threadID(), timeout)
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return codec.DecodeMapTryPutResponse(response), nil
		}
	}
}

func (m *MapImpl) tryRemove(key interface{}, timeout int64) (interface{}, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeMapTryRemoveRequest(m.name, keyData, threadID(), timeout)
		if response, err := m.invokeOnKey(request, keyData); err != nil {
			return nil, err
		} else {
			return codec.DecodeMapTryRemoveResponse(response), nil
		}
	}
}

func (m *MapImpl) convertToObjects(valueDatas []pubserialization.Data) ([]interface{}, error) {
	values := make([]interface{}, len(valueDatas))
	for i, valueData := range valueDatas {
		if value, err := m.convertToObject(valueData); err != nil {
			return nil, err
		} else {
			values[i] = value
		}
	}
	return values, nil
}

func (m *MapImpl) makePartitionIDMapFromArray(items []interface{}) (map[int32][]proto.Pair, error) {
	ps := m.partitionService
	pairsMap := map[int32][]proto.Pair{}
	for i := 0; i < len(items)/2; i += 2 {
		key := items[i]
		value := items[i+1]
		if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
			return nil, err
		} else {
			arr := pairsMap[ps.GetPartitionID(keyData)]
			pairsMap[ps.GetPartitionID(keyData)] = append(arr, proto.NewPair(keyData, valueData))
		}
	}
	return pairsMap, nil
}

func makeListenerFlags(config *hztypes.MapEntryListenerConfig) int32 {
	var flags int32
	if config.NotifyEntryAdded {
		flags |= hztypes.NotifyEntryAdded
	}
	if config.NotifyEntryUpdated {
		flags |= hztypes.NotifyEntryUpdated
	}
	if config.NotifyEntryRemoved {
		flags |= hztypes.NotifyEntryRemoved
	}
	return flags
}
