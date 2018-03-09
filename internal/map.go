// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package internal

import (
	. "github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	"time"
)

type MapProxy struct {
	*proxy
}

func newMapProxy(client *HazelcastClient, serviceName *string, name *string) *MapProxy {
	return &MapProxy{&proxy{client, serviceName, name}}
}

func (imap *MapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := MapPutEncodeRequest(imap.name, keyData, valueData, threadId, ttlUnlimited)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToObjectAndError(responseMessage, err, MapPutDecodeResponse)
}
func (imap *MapProxy) TryPut(key interface{}, value interface{}) (ok bool, err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := MapTryPutEncodeRequest(imap.name, keyData, valueData, threadId, ttlUnlimited)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToBoolAndError(responseMessage, err, MapTryPutDecodeResponse)
}
func (imap *MapProxy) PutTransient(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) (err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	ttl = GetTimeInMilliSeconds(ttl, ttlTimeUnit)
	request := MapPutTransientEncodeRequest(imap.name, keyData, valueData, threadId, ttl)
	_, err = imap.InvokeOnKey(request, keyData)
	return err
}
func (imap *MapProxy) Get(key interface{}) (value interface{}, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := MapGetEncodeRequest(imap.name, keyData, threadId)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToObjectAndError(responseMessage, err, MapGetDecodeResponse)

}
func (imap *MapProxy) Remove(key interface{}) (value interface{}, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := MapRemoveEncodeRequest(imap.name, keyData, threadId)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToObjectAndError(responseMessage, err, MapRemoveDecodeResponse)

}
func (imap *MapProxy) RemoveIfSame(key interface{}, value interface{}) (ok bool, err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := MapRemoveIfSameEncodeRequest(imap.name, keyData, valueData, threadId)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToBoolAndError(responseMessage, err, MapRemoveIfSameDecodeResponse)

}
func (imap *MapProxy) RemoveAll(predicate IPredicate) (err error) {
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return err
	}
	request := MapRemoveAllEncodeRequest(imap.name, predicateData)
	_, err = imap.InvokeOnRandomTarget(request)
	return err
}
func (imap *MapProxy) TryRemove(key interface{}, timeout int64, timeoutTimeUnit time.Duration) (ok bool, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	timeout = GetTimeInMilliSeconds(timeout, timeoutTimeUnit)
	request := MapTryRemoveEncodeRequest(imap.name, keyData, threadId, timeout)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToBoolAndError(responseMessage, err, MapTryRemoveDecodeResponse)
}
func (imap *MapProxy) Size() (size int32, err error) {
	request := MapSizeEncodeRequest(imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToInt32AndError(responseMessage, err, MapSizeDecodeResponse)

}
func (imap *MapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := MapContainsKeyEncodeRequest(imap.name, keyData, threadId)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToBoolAndError(responseMessage, err, MapContainsKeyDecodeResponse)

}
func (imap *MapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := imap.validateAndSerialize(value)
	if err != nil {
		return false, err
	}
	request := MapContainsValueEncodeRequest(imap.name, valueData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToBoolAndError(responseMessage, err, MapContainsValueDecodeResponse)

}
func (imap *MapProxy) Clear() (err error) {
	request := MapClearEncodeRequest(imap.name)
	_, err = imap.InvokeOnRandomTarget(request)
	return
}
func (imap *MapProxy) Delete(key interface{}) (err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := MapDeleteEncodeRequest(imap.name, keyData, threadId)
	_, err = imap.InvokeOnKey(request, keyData)
	return
}
func (imap *MapProxy) IsEmpty() (empty bool, err error) {
	request := MapIsEmptyEncodeRequest(imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToBoolAndError(responseMessage, err, MapIsEmptyDecodeResponse)

}
func (imap *MapProxy) AddIndex(attribute string, ordered bool) (err error) {
	request := MapAddIndexEncodeRequest(imap.name, &attribute, ordered)
	_, err = imap.InvokeOnRandomTarget(request)
	return
}
func (imap *MapProxy) Evict(key interface{}) (evicted bool, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := MapEvictEncodeRequest(imap.name, keyData, threadId)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToBoolAndError(responseMessage, err, MapEvictDecodeResponse)

}
func (imap *MapProxy) EvictAll() (err error) {
	request := MapEvictAllEncodeRequest(imap.name)
	_, err = imap.InvokeOnRandomTarget(request)
	return
}
func (imap *MapProxy) Flush() (err error) {
	request := MapFlushEncodeRequest(imap.name)
	_, err = imap.InvokeOnRandomTarget(request)
	return
}
func (imap *MapProxy) Lock(key interface{}) (err error) {
	return imap.LockWithLeaseTime(key, -1, time.Second)
}
func (imap *MapProxy) LockWithLeaseTime(key interface{}, lease int64, leaseTimeUnit time.Duration) (err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return err
	}
	lease = GetTimeInMilliSeconds(lease, leaseTimeUnit)
	request := MapLockEncodeRequest(imap.name, keyData, threadId, lease, imap.client.ProxyManager.nextReferenceId())
	_, err = imap.InvokeOnKey(request, keyData)
	return
}
func (imap *MapProxy) TryLock(key interface{}) (locked bool, err error) {
	return imap.TryLockWithTimeout(key, 0, time.Second)
}
func (imap *MapProxy) TryLockWithTimeout(key interface{}, timeout int64, timeoutTimeUnit time.Duration) (locked bool, err error) {
	return imap.TryLockWithTimeoutAndLease(key, timeout, timeoutTimeUnit, -1, time.Second)
}
func (imap *MapProxy) TryLockWithTimeoutAndLease(key interface{}, timeout int64, timeoutTimeUnit time.Duration, lease int64, leaseTimeUnit time.Duration) (locked bool, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	timeout = GetTimeInMilliSeconds(timeout, timeoutTimeUnit)
	lease = GetTimeInMilliSeconds(lease, leaseTimeUnit)
	request := MapTryLockEncodeRequest(imap.name, keyData, threadId, lease, timeout, imap.client.ProxyManager.nextReferenceId())
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToBoolAndError(responseMessage, err, MapTryLockDecodeResponse)
}
func (imap *MapProxy) Unlock(key interface{}) (err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := MapUnlockEncodeRequest(imap.name, keyData, threadId, imap.client.ProxyManager.nextReferenceId())
	_, err = imap.InvokeOnKey(request, keyData)
	return
}
func (imap *MapProxy) ForceUnlock(key interface{}) (err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := MapForceUnlockEncodeRequest(imap.name, keyData, imap.client.ProxyManager.nextReferenceId())
	_, err = imap.InvokeOnKey(request, keyData)
	return
}
func (imap *MapProxy) IsLocked(key interface{}) (locked bool, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := MapIsLockedEncodeRequest(imap.name, keyData)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToBoolAndError(responseMessage, err, MapIsLockedDecodeResponse)

}
func (imap *MapProxy) Replace(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := MapReplaceEncodeRequest(imap.name, keyData, valueData, threadId)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToObjectAndError(responseMessage, err, MapReplaceDecodeResponse)

}
func (imap *MapProxy) ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (replaced bool, err error) {
	keyData, oldValueData, newValueData, err := imap.validateAndSerialize3(key, oldValue, newValue)
	if err != nil {
		return false, err
	}
	request := MapReplaceIfSameEncodeRequest(imap.name, keyData, oldValueData, newValueData, threadId)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToBoolAndError(responseMessage, err, MapReplaceIfSameDecodeResponse)

}
func (imap *MapProxy) Set(key interface{}, value interface{}) (err error) {
	return imap.SetWithTtl(key, value, ttlUnlimited, time.Second)
}
func (imap *MapProxy) SetWithTtl(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) (err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	ttl = GetTimeInMilliSeconds(ttl, ttlTimeUnit)
	request := MapSetEncodeRequest(imap.name, keyData, valueData, threadId, ttl)
	_, err = imap.InvokeOnKey(request, keyData)
	return
}

func (imap *MapProxy) PutIfAbsent(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := MapPutIfAbsentEncodeRequest(imap.name, keyData, valueData, threadId, ttlUnlimited)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToObjectAndError(responseMessage, err, MapPutIfAbsentDecodeResponse)

}
func (imap *MapProxy) PutAll(entries map[interface{}]interface{}) (err error) {
	if entries == nil {
		return NewHazelcastNilPointerError(NIL_MAP_IS_NOT_ALLOWED, nil)
	}
	partitions, err := imap.validateAndSerializeMapAndGetPartitions(entries)
	if err != nil {
		return err
	}
	for partitionId, entryList := range partitions {
		request := MapPutAllEncodeRequest(imap.name, entryList)
		_, err = imap.InvokeOnPartition(request, partitionId)
		if err != nil {
			return err
		}
	}
	return nil
}
func (imap *MapProxy) KeySet() (keySet []interface{}, err error) {
	request := MapKeySetEncodeRequest(imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToInterfaceSliceAndError(responseMessage, err, MapKeySetDecodeResponse)
}
func (imap *MapProxy) KeySetWithPredicate(predicate IPredicate) (keySet []interface{}, err error) {
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := MapKeySetWithPredicateEncodeRequest(imap.name, predicateData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToInterfaceSliceAndError(responseMessage, err, MapKeySetWithPredicateDecodeResponse)
}
func (imap *MapProxy) Values() (values []interface{}, err error) {
	request := MapValuesEncodeRequest(imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToInterfaceSliceAndError(responseMessage, err, MapValuesDecodeResponse)
}
func (imap *MapProxy) ValuesWithPredicate(predicate IPredicate) (values []interface{}, err error) {
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := MapValuesWithPredicateEncodeRequest(imap.name, predicateData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToInterfaceSliceAndError(responseMessage, err, MapValuesWithPredicateDecodeResponse)
}
func (imap *MapProxy) EntrySet() (resultPairs []IPair, err error) {
	request := MapEntrySetEncodeRequest(imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToPairSliceAndError(responseMessage, err, MapEntrySetDecodeResponse)
}
func (imap *MapProxy) EntrySetWithPredicate(predicate IPredicate) (resultPairs []IPair, err error) {
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := MapEntriesWithPredicateEncodeRequest(imap.name, predicateData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToPairSliceAndError(responseMessage, err, MapEntriesWithPredicateDecodeResponse)
}
func (imap *MapProxy) GetAll(keys []interface{}) (entryMap map[interface{}]interface{}, err error) {
	if keys == nil {
		return nil, NewHazelcastNilPointerError(NIL_KEYS_ARE_NOT_ALLOWED, nil)
	}
	partitions := make(map[int32][]*serialization.Data)
	entryMap = make(map[interface{}]interface{}, 0)
	for _, key := range keys {
		keyData, err := imap.validateAndSerialize(key)
		if err != nil {
			return nil, err
		}
		partitionId := imap.client.PartitionService.GetPartitionId(keyData)
		partitions[partitionId] = append(partitions[partitionId], keyData)
	}
	for partitionId, keyList := range partitions {
		request := MapGetAllEncodeRequest(imap.name, keyList)
		responseMessage, err := imap.InvokeOnPartition(request, partitionId)
		if err != nil {
			return nil, err
		}
		response := MapGetAllDecodeResponse(responseMessage)()
		for _, pairData := range response {
			key, err := imap.ToObject(pairData.Key().(*serialization.Data))
			if err != nil {
				return nil, err
			}
			value, err := imap.ToObject(pairData.Value().(*serialization.Data))
			if err != nil {
				return nil, err
			}
			entryMap[key] = value
		}
	}
	return entryMap, nil
}
func (imap *MapProxy) GetEntryView(key interface{}) (entryView IEntryView, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := MapGetEntryViewEncodeRequest(imap.name, keyData, threadId)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	response := MapGetEntryViewDecodeResponse(responseMessage)()
	resultKey, _ := imap.ToObject(response.KeyData())
	resultValue, _ := imap.ToObject(response.ValueData())
	entryView = NewEntryView(resultKey, resultValue, response.Cost(),
		response.CreationTime(), response.ExpirationTime(), response.Hits(), response.LastAccessTime(), response.LastStoredTime(),
		response.LastUpdateTime(), response.Version(), response.EvictionCriteriaNumber(), response.Ttl())
	return entryView, nil
}
func (imap *MapProxy) AddEntryListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	var request *ClientMessage
	listenerFlags := GetEntryListenerFlags(listener)
	request = MapAddEntryListenerEncodeRequest(imap.name, includeValue, listenerFlags, imap.isSmart())
	eventHandler := func(clientMessage *ClientMessage) {
		MapAddEntryListenerHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *ClientMessage {
		return MapRemoveEntryListenerEncodeRequest(imap.name, registrationId)
	}, func(clientMessage *ClientMessage) *string {
		return MapAddEntryListenerDecodeResponse(clientMessage)()
	})
}
func (imap *MapProxy) AddEntryListenerWithPredicate(listener interface{}, predicate IPredicate, includeValue bool) (*string, error) {
	var request *ClientMessage
	listenerFlags := GetEntryListenerFlags(listener)
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request = MapAddEntryListenerWithPredicateEncodeRequest(imap.name, predicateData, includeValue, listenerFlags, false)
	eventHandler := func(clientMessage *ClientMessage) {
		MapAddEntryListenerWithPredicateHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *ClientMessage {
			return MapRemoveEntryListenerEncodeRequest(imap.name, registrationId)
		}, func(clientMessage *ClientMessage) *string {
			return MapAddEntryListenerWithPredicateDecodeResponse(clientMessage)()
		})
}
func (imap *MapProxy) AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID *string, err error) {
	var request *ClientMessage
	listenerFlags := GetEntryListenerFlags(listener)
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request = MapAddEntryListenerToKeyEncodeRequest(imap.name, keyData, includeValue, listenerFlags, imap.isSmart())
	eventHandler := func(clientMessage *ClientMessage) {
		MapAddEntryListenerToKeyHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *ClientMessage {
		return MapRemoveEntryListenerEncodeRequest(imap.name, registrationId)
	}, func(clientMessage *ClientMessage) *string {
		return MapAddEntryListenerToKeyDecodeResponse(clientMessage)()
	})
}
func (imap *MapProxy) AddEntryListenerToKeyWithPredicate(listener interface{}, predicate IPredicate, key interface{}, includeValue bool) (*string, error) {
	var request *ClientMessage
	listenerFlags := GetEntryListenerFlags(listener)
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request = MapAddEntryListenerToKeyWithPredicateEncodeRequest(imap.name, keyData, predicateData, includeValue, listenerFlags, false)
	eventHandler := func(clientMessage *ClientMessage) {
		MapAddEntryListenerToKeyWithPredicateHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *ClientMessage {
			return MapRemoveEntryListenerEncodeRequest(imap.name, registrationId)
		}, func(clientMessage *ClientMessage) *string {
			return MapAddEntryListenerToKeyWithPredicateDecodeResponse(clientMessage)()
		})
}
func (imap *MapProxy) onEntryEvent(keyData *serialization.Data, oldValueData *serialization.Data, valueData *serialization.Data, mergingValueData *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32, includedValue bool, listener interface{}) {
	key, _ := imap.ToObject(keyData)
	oldValue, _ := imap.ToObject(oldValueData)
	value, _ := imap.ToObject(valueData)
	mergingValue, _ := imap.ToObject(mergingValueData)
	entryEvent := NewEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid)
	mapEvent := NewMapEvent(eventType, Uuid, numberOfAffectedEntries)
	switch eventType {
	case ENTRYEVENT_ADDED:
		listener.(EntryAddedListener).EntryAdded(entryEvent)
	case ENTRYEVENT_REMOVED:
		listener.(EntryRemovedListener).EntryRemoved(entryEvent)
	case ENTRYEVENT_UPDATED:
		listener.(EntryUpdatedListener).EntryUpdated(entryEvent)
	case ENTRYEVENT_EVICTED:
		listener.(EntryEvictedListener).EntryEvicted(entryEvent)
	case ENTRYEVENT_EVICT_ALL:
		listener.(EntryEvictAllListener).EntryEvictAll(mapEvent)
	case ENTRYEVENT_CLEAR_ALL:
		listener.(EntryClearAllListener).EntryClearAll(mapEvent)
	case ENTRYEVENT_MERGED:
		listener.(EntryMergedListener).EntryMerged(entryEvent)
	case ENTRYEVENT_EXPIRED:
		listener.(EntryExpiredListener).EntryExpired(entryEvent)
	}
}

func (imap *MapProxy) RemoveEntryListener(registrationId *string) (bool, error) {
	return imap.client.ListenerService.deregisterListener(*registrationId, func(registrationId *string) *ClientMessage {
		return MapRemoveEntryListenerEncodeRequest(imap.name, registrationId)
	})
}

func (imap *MapProxy) ExecuteOnKey(key interface{}, entryProcessor interface{}) (result interface{}, err error) {
	keyData, entryProcessorData, err := imap.validateAndSerialize2(key, entryProcessor)
	if err != nil {
		return nil, err
	}
	request := MapExecuteOnKeyEncodeRequest(imap.name, entryProcessorData, keyData, threadId)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	return imap.DecodeToObjectAndError(responseMessage, err, MapExecuteOnKeyDecodeResponse)
}
func (imap *MapProxy) ExecuteOnKeys(keys []interface{}, entryProcessor interface{}) (keyToResultPairs []IPair, err error) {
	keysData := make([]*serialization.Data, len(keys))
	for index, key := range keys {
		keyData, err := imap.validateAndSerialize(key)
		if err != nil {
			return nil, err
		}
		keysData[index] = keyData
	}
	entryProcessorData, err := imap.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := MapExecuteOnKeysEncodeRequest(imap.name, entryProcessorData, keysData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToPairSliceAndError(responseMessage, err, MapExecuteOnKeysDecodeResponse)
}
func (imap *MapProxy) ExecuteOnEntries(entryProcessor interface{}) (keyToResultPairs []IPair, err error) {
	entryProcessorData, err := imap.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := MapExecuteOnAllKeysEncodeRequest(imap.name, entryProcessorData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToPairSliceAndError(responseMessage, err, MapExecuteOnAllKeysDecodeResponse)
}

func (imap *MapProxy) ExecuteOnEntriesWithPredicate(entryProcessor interface{}, predicate IPredicate) (keyToResultPairs []IPair, err error) {
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	entryProcessorData, err := imap.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := MapExecuteWithPredicateEncodeRequest(imap.name, entryProcessorData, predicateData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	return imap.DecodeToPairSliceAndError(responseMessage, err, MapExecuteWithPredicateDecodeResponse)
}
