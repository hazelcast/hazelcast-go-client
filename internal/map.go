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
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type MapProxy struct {
	*proxy
}

func newMapProxy(client *HazelcastClient, serviceName *string, name *string) (*MapProxy, error) {
	return &MapProxy{&proxy{client, serviceName, name}}, nil
}

func (mp *MapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := protocol.MapPutEncodeRequest(mp.name, keyData, valueData, threadId, ttlUnlimited)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapPutDecodeResponse)
}
func (mp *MapProxy) TryPut(key interface{}, value interface{}) (ok bool, err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := protocol.MapTryPutEncodeRequest(mp.name, keyData, valueData, threadId, ttlUnlimited)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapTryPutDecodeResponse)
}
func (mp *MapProxy) PutTransient(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) (err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	ttl = common.GetTimeInMilliSeconds(ttl, ttlTimeUnit)
	request := protocol.MapPutTransientEncodeRequest(mp.name, keyData, valueData, threadId, ttl)
	_, err = mp.invokeOnKey(request, keyData)
	return err
}
func (mp *MapProxy) Get(key interface{}) (value interface{}, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.MapGetEncodeRequest(mp.name, keyData, threadId)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapGetDecodeResponse)

}
func (mp *MapProxy) Remove(key interface{}) (value interface{}, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.MapRemoveEncodeRequest(mp.name, keyData, threadId)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapRemoveDecodeResponse)

}
func (mp *MapProxy) RemoveIfSame(key interface{}, value interface{}) (ok bool, err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := protocol.MapRemoveIfSameEncodeRequest(mp.name, keyData, valueData, threadId)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapRemoveIfSameDecodeResponse)

}
func (mp *MapProxy) RemoveAll(predicate interface{}) (err error) {
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return err
	}
	request := protocol.MapRemoveAllEncodeRequest(mp.name, predicateData)
	_, err = mp.invokeOnRandomTarget(request)
	return err
}
func (mp *MapProxy) TryRemove(key interface{}, timeout int64, timeoutTimeUnit time.Duration) (ok bool, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	timeout = common.GetTimeInMilliSeconds(timeout, timeoutTimeUnit)
	request := protocol.MapTryRemoveEncodeRequest(mp.name, keyData, threadId, timeout)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapTryRemoveDecodeResponse)
}
func (mp *MapProxy) Size() (size int32, err error) {
	request := protocol.MapSizeEncodeRequest(mp.name)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInt32AndError(responseMessage, err, protocol.MapSizeDecodeResponse)

}
func (mp *MapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.MapContainsKeyEncodeRequest(mp.name, keyData, threadId)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapContainsKeyDecodeResponse)

}
func (mp *MapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := mp.validateAndSerialize(value)
	if err != nil {
		return false, err
	}
	request := protocol.MapContainsValueEncodeRequest(mp.name, valueData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapContainsValueDecodeResponse)

}
func (mp *MapProxy) Clear() (err error) {
	request := protocol.MapClearEncodeRequest(mp.name)
	_, err = mp.invokeOnRandomTarget(request)
	return
}
func (mp *MapProxy) Delete(key interface{}) (err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MapDeleteEncodeRequest(mp.name, keyData, threadId)
	_, err = mp.invokeOnKey(request, keyData)
	return
}
func (mp *MapProxy) IsEmpty() (empty bool, err error) {
	request := protocol.MapIsEmptyEncodeRequest(mp.name)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapIsEmptyDecodeResponse)

}
func (mp *MapProxy) AddIndex(attribute string, ordered bool) (err error) {
	request := protocol.MapAddIndexEncodeRequest(mp.name, &attribute, ordered)
	_, err = mp.invokeOnRandomTarget(request)
	return
}
func (mp *MapProxy) Evict(key interface{}) (evicted bool, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.MapEvictEncodeRequest(mp.name, keyData, threadId)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapEvictDecodeResponse)

}
func (mp *MapProxy) EvictAll() (err error) {
	request := protocol.MapEvictAllEncodeRequest(mp.name)
	_, err = mp.invokeOnRandomTarget(request)
	return
}
func (mp *MapProxy) Flush() (err error) {
	request := protocol.MapFlushEncodeRequest(mp.name)
	_, err = mp.invokeOnRandomTarget(request)
	return
}
func (mp *MapProxy) Lock(key interface{}) (err error) {
	return mp.LockWithLeaseTime(key, -1, time.Second)
}
func (mp *MapProxy) LockWithLeaseTime(key interface{}, lease int64, leaseTimeUnit time.Duration) (err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	lease = common.GetTimeInMilliSeconds(lease, leaseTimeUnit)
	request := protocol.MapLockEncodeRequest(mp.name, keyData, threadId, lease, mp.client.ProxyManager.nextReferenceId())
	_, err = mp.invokeOnKey(request, keyData)
	return
}
func (mp *MapProxy) TryLock(key interface{}) (locked bool, err error) {
	return mp.TryLockWithTimeout(key, 0, time.Second)
}
func (mp *MapProxy) TryLockWithTimeout(key interface{}, timeout int64, timeoutTimeUnit time.Duration) (locked bool, err error) {
	return mp.TryLockWithTimeoutAndLease(key, timeout, timeoutTimeUnit, -1, time.Second)
}
func (mp *MapProxy) TryLockWithTimeoutAndLease(key interface{}, timeout int64, timeoutTimeUnit time.Duration, lease int64, leaseTimeUnit time.Duration) (locked bool, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	timeout = common.GetTimeInMilliSeconds(timeout, timeoutTimeUnit)
	lease = common.GetTimeInMilliSeconds(lease, leaseTimeUnit)
	request := protocol.MapTryLockEncodeRequest(mp.name, keyData, threadId, lease, timeout, mp.client.ProxyManager.nextReferenceId())
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapTryLockDecodeResponse)
}
func (mp *MapProxy) Unlock(key interface{}) (err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MapUnlockEncodeRequest(mp.name, keyData, threadId, mp.client.ProxyManager.nextReferenceId())
	_, err = mp.invokeOnKey(request, keyData)
	return
}
func (mp *MapProxy) ForceUnlock(key interface{}) (err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MapForceUnlockEncodeRequest(mp.name, keyData, mp.client.ProxyManager.nextReferenceId())
	_, err = mp.invokeOnKey(request, keyData)
	return
}
func (mp *MapProxy) IsLocked(key interface{}) (locked bool, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.MapIsLockedEncodeRequest(mp.name, keyData)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapIsLockedDecodeResponse)

}
func (mp *MapProxy) Replace(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := protocol.MapReplaceEncodeRequest(mp.name, keyData, valueData, threadId)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapReplaceDecodeResponse)

}
func (mp *MapProxy) ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (replaced bool, err error) {
	keyData, oldValueData, newValueData, err := mp.validateAndSerialize3(key, oldValue, newValue)
	if err != nil {
		return false, err
	}
	request := protocol.MapReplaceIfSameEncodeRequest(mp.name, keyData, oldValueData, newValueData, threadId)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapReplaceIfSameDecodeResponse)

}
func (mp *MapProxy) Set(key interface{}, value interface{}) (err error) {
	return mp.SetWithTtl(key, value, ttlUnlimited, time.Second)
}
func (mp *MapProxy) SetWithTtl(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) (err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	ttl = common.GetTimeInMilliSeconds(ttl, ttlTimeUnit)
	request := protocol.MapSetEncodeRequest(mp.name, keyData, valueData, threadId, ttl)
	_, err = mp.invokeOnKey(request, keyData)
	return
}

func (mp *MapProxy) PutIfAbsent(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := protocol.MapPutIfAbsentEncodeRequest(mp.name, keyData, valueData, threadId, ttlUnlimited)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapPutIfAbsentDecodeResponse)

}
func (mp *MapProxy) PutAll(entries map[interface{}]interface{}) (err error) {
	if entries == nil {
		return core.NewHazelcastNilPointerError(common.NilMapIsNotAllowed, nil)
	}
	partitions, err := mp.validateAndSerializeMapAndGetPartitions(entries)
	if err != nil {
		return err
	}
	for partitionId, entryList := range partitions {
		request := protocol.MapPutAllEncodeRequest(mp.name, entryList)
		_, err = mp.invokeOnPartition(request, partitionId)
		if err != nil {
			return err
		}
	}
	return nil
}
func (mp *MapProxy) KeySet() (keySet []interface{}, err error) {
	request := protocol.MapKeySetEncodeRequest(mp.name)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapKeySetDecodeResponse)
}
func (mp *MapProxy) KeySetWithPredicate(predicate interface{}) (keySet []interface{}, err error) {
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := protocol.MapKeySetWithPredicateEncodeRequest(mp.name, predicateData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapKeySetWithPredicateDecodeResponse)
}
func (mp *MapProxy) Values() (values []interface{}, err error) {
	request := protocol.MapValuesEncodeRequest(mp.name)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapValuesDecodeResponse)
}
func (mp *MapProxy) ValuesWithPredicate(predicate interface{}) (values []interface{}, err error) {
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := protocol.MapValuesWithPredicateEncodeRequest(mp.name, predicateData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapValuesWithPredicateDecodeResponse)
}
func (mp *MapProxy) EntrySet() (resultPairs []core.IPair, err error) {
	request := protocol.MapEntrySetEncodeRequest(mp.name)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToPairSliceAndError(responseMessage, err, protocol.MapEntrySetDecodeResponse)
}
func (mp *MapProxy) EntrySetWithPredicate(predicate interface{}) (resultPairs []core.IPair, err error) {
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := protocol.MapEntriesWithPredicateEncodeRequest(mp.name, predicateData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToPairSliceAndError(responseMessage, err, protocol.MapEntriesWithPredicateDecodeResponse)
}
func (mp *MapProxy) GetAll(keys []interface{}) (entryMap map[interface{}]interface{}, err error) {
	if keys == nil {
		return nil, core.NewHazelcastNilPointerError(common.NilKeysAreNotAllowed, nil)
	}
	partitions := make(map[int32][]*serialization.Data)
	entryMap = make(map[interface{}]interface{}, 0)
	for _, key := range keys {
		keyData, err := mp.validateAndSerialize(key)
		if err != nil {
			return nil, err
		}
		partitionId := mp.client.PartitionService.GetPartitionId(keyData)
		partitions[partitionId] = append(partitions[partitionId], keyData)
	}
	for partitionId, keyList := range partitions {
		request := protocol.MapGetAllEncodeRequest(mp.name, keyList)
		responseMessage, err := mp.invokeOnPartition(request, partitionId)
		if err != nil {
			return nil, err
		}
		response := protocol.MapGetAllDecodeResponse(responseMessage)()
		for _, pairData := range response {
			key, err := mp.toObject(pairData.Key().(*serialization.Data))
			if err != nil {
				return nil, err
			}
			value, err := mp.toObject(pairData.Value().(*serialization.Data))
			if err != nil {
				return nil, err
			}
			entryMap[key] = value
		}
	}
	return entryMap, nil
}
func (mp *MapProxy) GetEntryView(key interface{}) (entryView core.IEntryView, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.MapGetEntryViewEncodeRequest(mp.name, keyData, threadId)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	response := protocol.MapGetEntryViewDecodeResponse(responseMessage)()
	resultKey, _ := mp.toObject(response.KeyData())
	resultValue, _ := mp.toObject(response.ValueData())
	entryView = protocol.NewEntryView(resultKey, resultValue, response.Cost(),
		response.CreationTime(), response.ExpirationTime(), response.Hits(), response.LastAccessTime(), response.LastStoredTime(),
		response.LastUpdateTime(), response.Version(), response.EvictionCriteriaNumber(), response.Ttl())
	return entryView, nil
}
func (mp *MapProxy) AddEntryListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	var request *protocol.ClientMessage
	listenerFlags := protocol.GetEntryListenerFlags(listener)
	request = protocol.MapAddEntryListenerEncodeRequest(mp.name, includeValue, listenerFlags, mp.isSmart())
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			mp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return mp.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *protocol.ClientMessage {
		return protocol.MapRemoveEntryListenerEncodeRequest(mp.name, registrationId)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.MapAddEntryListenerDecodeResponse(clientMessage)()
	})
}
func (mp *MapProxy) AddEntryListenerWithPredicate(listener interface{}, predicate interface{}, includeValue bool) (*string, error) {
	var request *protocol.ClientMessage
	listenerFlags := protocol.GetEntryListenerFlags(listener)
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request = protocol.MapAddEntryListenerWithPredicateEncodeRequest(mp.name, predicateData, includeValue, listenerFlags, false)
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerWithPredicateHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			mp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return mp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *protocol.ClientMessage {
			return protocol.MapRemoveEntryListenerEncodeRequest(mp.name, registrationId)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.MapAddEntryListenerWithPredicateDecodeResponse(clientMessage)()
		})
}
func (mp *MapProxy) AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID *string, err error) {
	var request *protocol.ClientMessage
	listenerFlags := protocol.GetEntryListenerFlags(listener)
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request = protocol.MapAddEntryListenerToKeyEncodeRequest(mp.name, keyData, includeValue, listenerFlags, mp.isSmart())
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerToKeyHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			mp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return mp.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *protocol.ClientMessage {
		return protocol.MapRemoveEntryListenerEncodeRequest(mp.name, registrationId)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.MapAddEntryListenerToKeyDecodeResponse(clientMessage)()
	})
}
func (mp *MapProxy) AddEntryListenerToKeyWithPredicate(listener interface{}, predicate interface{}, key interface{}, includeValue bool) (*string, error) {
	var request *protocol.ClientMessage
	listenerFlags := protocol.GetEntryListenerFlags(listener)
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request = protocol.MapAddEntryListenerToKeyWithPredicateEncodeRequest(mp.name, keyData, predicateData, includeValue, listenerFlags, false)
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerToKeyWithPredicateHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			mp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return mp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *protocol.ClientMessage {
			return protocol.MapRemoveEntryListenerEncodeRequest(mp.name, registrationId)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.MapAddEntryListenerToKeyWithPredicateDecodeResponse(clientMessage)()
		})
}
func (mp *MapProxy) onEntryEvent(keyData *serialization.Data, oldValueData *serialization.Data, valueData *serialization.Data, mergingValueData *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32, includedValue bool, listener interface{}) {
	key, _ := mp.toObject(keyData)
	oldValue, _ := mp.toObject(oldValueData)
	value, _ := mp.toObject(valueData)
	mergingValue, _ := mp.toObject(mergingValueData)
	entryEvent := protocol.NewEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid)
	mapEvent := protocol.NewMapEvent(eventType, Uuid, numberOfAffectedEntries)
	switch eventType {
	case common.EntryEventAdded:
		listener.(protocol.EntryAddedListener).EntryAdded(entryEvent)
	case common.EntryEventRemoved:
		listener.(protocol.EntryRemovedListener).EntryRemoved(entryEvent)
	case common.EntryEventUpdated:
		listener.(protocol.EntryUpdatedListener).EntryUpdated(entryEvent)
	case common.EntryEventEvicted:
		listener.(protocol.EntryEvictedListener).EntryEvicted(entryEvent)
	case common.EntryEventEvictAll:
		listener.(protocol.EntryEvictAllListener).EntryEvictAll(mapEvent)
	case common.EntryEventClearAll:
		listener.(protocol.EntryClearAllListener).EntryClearAll(mapEvent)
	case common.EntryEventMerged:
		listener.(protocol.EntryMergedListener).EntryMerged(entryEvent)
	case common.EntryEventExpired:
		listener.(protocol.EntryExpiredListener).EntryExpired(entryEvent)
	}
}

func (mp *MapProxy) RemoveEntryListener(registrationId *string) (bool, error) {
	return mp.client.ListenerService.deregisterListener(*registrationId, func(registrationId *string) *protocol.ClientMessage {
		return protocol.MapRemoveEntryListenerEncodeRequest(mp.name, registrationId)
	})
}

func (mp *MapProxy) ExecuteOnKey(key interface{}, entryProcessor interface{}) (result interface{}, err error) {
	keyData, entryProcessorData, err := mp.validateAndSerialize2(key, entryProcessor)
	if err != nil {
		return nil, err
	}
	request := protocol.MapExecuteOnKeyEncodeRequest(mp.name, entryProcessorData, keyData, threadId)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapExecuteOnKeyDecodeResponse)
}
func (mp *MapProxy) ExecuteOnKeys(keys []interface{}, entryProcessor interface{}) (keyToResultPairs []core.IPair, err error) {
	keysData := make([]*serialization.Data, len(keys))
	for index, key := range keys {
		keyData, err := mp.validateAndSerialize(key)
		if err != nil {
			return nil, err
		}
		keysData[index] = keyData
	}
	entryProcessorData, err := mp.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := protocol.MapExecuteOnKeysEncodeRequest(mp.name, entryProcessorData, keysData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToPairSliceAndError(responseMessage, err, protocol.MapExecuteOnKeysDecodeResponse)
}
func (mp *MapProxy) ExecuteOnEntries(entryProcessor interface{}) (keyToResultPairs []core.IPair, err error) {
	entryProcessorData, err := mp.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := protocol.MapExecuteOnAllKeysEncodeRequest(mp.name, entryProcessorData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToPairSliceAndError(responseMessage, err, protocol.MapExecuteOnAllKeysDecodeResponse)
}

func (mp *MapProxy) ExecuteOnEntriesWithPredicate(entryProcessor interface{}, predicate interface{}) (keyToResultPairs []core.IPair, err error) {
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	entryProcessorData, err := mp.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := protocol.MapExecuteWithPredicateEncodeRequest(mp.name, entryProcessorData, predicateData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToPairSliceAndError(responseMessage, err, protocol.MapExecuteWithPredicateDecodeResponse)
}
