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
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/timeutil"
)

type mapProxy struct {
	*proxy
}

func newMapProxy(client *HazelcastClient, serviceName *string, name *string) (*mapProxy, error) {
	return &mapProxy{&proxy{client, serviceName, name}}, nil
}

func (mp *mapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := protocol.MapPutEncodeRequest(mp.name, keyData, valueData, threadID, ttlUnlimited)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapPutDecodeResponse)
}

func (mp *mapProxy) TryPut(key interface{}, value interface{}) (ok bool, err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := protocol.MapTryPutEncodeRequest(mp.name, keyData, valueData, threadID, ttlUnlimited)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapTryPutDecodeResponse)
}

func (mp *mapProxy) PutTransient(key interface{}, value interface{}, ttl time.Duration) (err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	ttlInMillis := timeutil.GetTimeInMilliSeconds(ttl)
	request := protocol.MapPutTransientEncodeRequest(mp.name, keyData, valueData, threadID, ttlInMillis)
	_, err = mp.invokeOnKey(request, keyData)
	return err
}

func (mp *mapProxy) Get(key interface{}) (value interface{}, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.MapGetEncodeRequest(mp.name, keyData, threadID)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapGetDecodeResponse)
}

func (mp *mapProxy) Remove(key interface{}) (value interface{}, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.MapRemoveEncodeRequest(mp.name, keyData, threadID)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapRemoveDecodeResponse)
}

func (mp *mapProxy) RemoveIfSame(key interface{}, value interface{}) (ok bool, err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := protocol.MapRemoveIfSameEncodeRequest(mp.name, keyData, valueData, threadID)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapRemoveIfSameDecodeResponse)
}

func (mp *mapProxy) RemoveAll(predicate interface{}) (err error) {
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return err
	}
	request := protocol.MapRemoveAllEncodeRequest(mp.name, predicateData)
	_, err = mp.invokeOnRandomTarget(request)
	return err
}

func (mp *mapProxy) TryRemove(key interface{}, timeout time.Duration) (ok bool, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	timeoutInMillis := timeutil.GetTimeInMilliSeconds(timeout)
	request := protocol.MapTryRemoveEncodeRequest(mp.name, keyData, threadID, timeoutInMillis)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapTryRemoveDecodeResponse)
}

func (mp *mapProxy) Size() (size int32, err error) {
	request := protocol.MapSizeEncodeRequest(mp.name)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInt32AndError(responseMessage, err, protocol.MapSizeDecodeResponse)
}

func (mp *mapProxy) Aggregate(aggregator interface{}) (result interface{}, err error) {
	panic("implement me")
}

func (mp *mapProxy) AggregateWithPredicate(aggregator interface{}, predicate interface{}) (result interface{}, err error) {
	panic("implement me")
}

func (mp *mapProxy) Project(projection interface{}) (result []interface{}, err error) {
	// TODO checkNotPagingPredicate when PagingPredicate is implemented.
	projectionData, err := mp.validateAndSerialize(projection)
	if err != nil {
		return
	}
	request := protocol.MapProjectEncodeRequest(mp.name, projectionData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapProjectDecodeResponse)
}

func (mp *mapProxy) ProjectWithPredicate(projection interface{}, predicate interface{}) (result []interface{}, err error) {
	// TODO checkNotPagingPredicate when PagingPredicate is implemented.
	projectionData, err := mp.validateAndSerialize(projection)
	if err != nil {
		return
	}
	predicateData, err := mp.validateAndSerialize(predicate)
	if err != nil {
		return
	}
	request := protocol.MapProjectWithPredicateEncodeRequest(mp.name, projectionData, predicateData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapProjectWithPredicateDecodeResponse)
}

func (mp *mapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.MapContainsKeyEncodeRequest(mp.name, keyData, threadID)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapContainsKeyDecodeResponse)
}

func (mp *mapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := mp.validateAndSerialize(value)
	if err != nil {
		return false, err
	}
	request := protocol.MapContainsValueEncodeRequest(mp.name, valueData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapContainsValueDecodeResponse)
}

func (mp *mapProxy) Clear() (err error) {
	request := protocol.MapClearEncodeRequest(mp.name)
	_, err = mp.invokeOnRandomTarget(request)
	return
}

func (mp *mapProxy) Delete(key interface{}) (err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MapDeleteEncodeRequest(mp.name, keyData, threadID)
	_, err = mp.invokeOnKey(request, keyData)
	return
}

func (mp *mapProxy) IsEmpty() (empty bool, err error) {
	request := protocol.MapIsEmptyEncodeRequest(mp.name)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapIsEmptyDecodeResponse)
}

func (mp *mapProxy) AddIndex(attribute string, ordered bool) (err error) {
	request := protocol.MapAddIndexEncodeRequest(mp.name, &attribute, ordered)
	_, err = mp.invokeOnRandomTarget(request)
	return
}

func (mp *mapProxy) Evict(key interface{}) (evicted bool, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.MapEvictEncodeRequest(mp.name, keyData, threadID)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapEvictDecodeResponse)
}

func (mp *mapProxy) EvictAll() (err error) {
	request := protocol.MapEvictAllEncodeRequest(mp.name)
	_, err = mp.invokeOnRandomTarget(request)
	return
}

func (mp *mapProxy) Flush() (err error) {
	request := protocol.MapFlushEncodeRequest(mp.name)
	_, err = mp.invokeOnRandomTarget(request)
	return
}

func (mp *mapProxy) Lock(key interface{}) (err error) {
	return mp.LockWithLeaseTime(key, -1)
}

func (mp *mapProxy) LockWithLeaseTime(key interface{}, lease time.Duration) (err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	leaseInMillis := timeutil.GetTimeInMilliSeconds(lease)
	request := protocol.MapLockEncodeRequest(mp.name, keyData, threadID, leaseInMillis, mp.client.ProxyManager.nextReferenceID())
	_, err = mp.invokeOnKey(request, keyData)
	return
}

func (mp *mapProxy) TryLock(key interface{}) (locked bool, err error) {
	return mp.TryLockWithTimeout(key, 0)
}

func (mp *mapProxy) TryLockWithTimeout(key interface{}, timeout time.Duration) (locked bool, err error) {
	return mp.TryLockWithTimeoutAndLease(key, timeout, -1)
}

func (mp *mapProxy) TryLockWithTimeoutAndLease(key interface{}, timeout time.Duration, lease time.Duration) (
	locked bool, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	timeoutInMillis := timeutil.GetTimeInMilliSeconds(timeout)
	leaseInMillis := timeutil.GetTimeInMilliSeconds(lease)
	request := protocol.MapTryLockEncodeRequest(mp.name, keyData, threadID, leaseInMillis, timeoutInMillis,
		mp.client.ProxyManager.nextReferenceID())
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapTryLockDecodeResponse)
}

func (mp *mapProxy) Unlock(key interface{}) (err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MapUnlockEncodeRequest(mp.name, keyData, threadID, mp.client.ProxyManager.nextReferenceID())
	_, err = mp.invokeOnKey(request, keyData)
	return
}

func (mp *mapProxy) ForceUnlock(key interface{}) (err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MapForceUnlockEncodeRequest(mp.name, keyData, mp.client.ProxyManager.nextReferenceID())
	_, err = mp.invokeOnKey(request, keyData)
	return
}

func (mp *mapProxy) IsLocked(key interface{}) (locked bool, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.MapIsLockedEncodeRequest(mp.name, keyData)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapIsLockedDecodeResponse)

}

func (mp *mapProxy) Replace(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := protocol.MapReplaceEncodeRequest(mp.name, keyData, valueData, threadID)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapReplaceDecodeResponse)

}

func (mp *mapProxy) ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (replaced bool, err error) {
	keyData, oldValueData, newValueData, err := mp.validateAndSerialize3(key, oldValue, newValue)
	if err != nil {
		return false, err
	}
	request := protocol.MapReplaceIfSameEncodeRequest(mp.name, keyData, oldValueData, newValueData, threadID)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToBoolAndError(responseMessage, err, protocol.MapReplaceIfSameDecodeResponse)

}

func (mp *mapProxy) Set(key interface{}, value interface{}) (err error) {
	return mp.SetWithTTL(key, value, ttlUnlimited)
}

func (mp *mapProxy) SetWithTTL(key interface{}, value interface{}, ttl time.Duration) (err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	ttlInMillis := timeutil.GetTimeInMilliSeconds(ttl)
	request := protocol.MapSetEncodeRequest(mp.name, keyData, valueData, threadID, ttlInMillis)
	_, err = mp.invokeOnKey(request, keyData)
	return
}

func (mp *mapProxy) PutIfAbsent(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := protocol.MapPutIfAbsentEncodeRequest(mp.name, keyData, valueData, threadID, ttlUnlimited)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapPutIfAbsentDecodeResponse)

}

func (mp *mapProxy) PutAll(entries map[interface{}]interface{}) (err error) {
	if entries == nil {
		return core.NewHazelcastNilPointerError(bufutil.NilMapIsNotAllowed, nil)
	}
	partitions, err := mp.validateAndSerializeMapAndGetPartitions(entries)
	if err != nil {
		return err
	}
	for partitionID, entryList := range partitions {
		request := protocol.MapPutAllEncodeRequest(mp.name, entryList)
		_, err = mp.invokeOnPartition(request, partitionID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mp *mapProxy) KeySet() (keySet []interface{}, err error) {
	request := protocol.MapKeySetEncodeRequest(mp.name)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapKeySetDecodeResponse)
}

func (mp *mapProxy) KeySetWithPredicate(predicate interface{}) (keySet []interface{}, err error) {
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := protocol.MapKeySetWithPredicateEncodeRequest(mp.name, predicateData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapKeySetWithPredicateDecodeResponse)
}

func (mp *mapProxy) Values() (values []interface{}, err error) {
	request := protocol.MapValuesEncodeRequest(mp.name)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapValuesDecodeResponse)
}

func (mp *mapProxy) ValuesWithPredicate(predicate interface{}) (values []interface{}, err error) {
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := protocol.MapValuesWithPredicateEncodeRequest(mp.name, predicateData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapValuesWithPredicateDecodeResponse)
}

func (mp *mapProxy) EntrySet() (resultPairs []core.Pair, err error) {
	request := protocol.MapEntrySetEncodeRequest(mp.name)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToPairSliceAndError(responseMessage, err, protocol.MapEntrySetDecodeResponse)
}

func (mp *mapProxy) EntrySetWithPredicate(predicate interface{}) (resultPairs []core.Pair, err error) {
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := protocol.MapEntriesWithPredicateEncodeRequest(mp.name, predicateData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToPairSliceAndError(responseMessage, err, protocol.MapEntriesWithPredicateDecodeResponse)
}

func (mp *mapProxy) GetAll(keys []interface{}) (entryMap map[interface{}]interface{}, err error) {
	if keys == nil {
		return nil, core.NewHazelcastNilPointerError(bufutil.NilKeysAreNotAllowed, nil)
	}
	partitions := make(map[int32][]*serialization.Data)
	entryMap = make(map[interface{}]interface{})
	for _, key := range keys {
		keyData, err := mp.validateAndSerialize(key)
		if err != nil {
			return nil, err
		}
		partitionID := mp.client.PartitionService.GetPartitionID(keyData)
		partitions[partitionID] = append(partitions[partitionID], keyData)
	}
	for partitionID, keyList := range partitions {
		request := protocol.MapGetAllEncodeRequest(mp.name, keyList)
		responseMessage, err := mp.invokeOnPartition(request, partitionID)
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

func (mp *mapProxy) GetEntryView(key interface{}) (entryView core.EntryView, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.MapGetEntryViewEncodeRequest(mp.name, keyData, threadID)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	response := protocol.MapGetEntryViewDecodeResponse(responseMessage)()
	resultKey, _ := mp.toObject(response.KeyData())
	resultValue, _ := mp.toObject(response.ValueData())
	entryView = protocol.NewEntryView(resultKey, resultValue, response.Cost(),
		response.CreationTime(), response.ExpirationTime(), response.Hits(), response.LastAccessTime(), response.LastStoredTime(),
		response.LastUpdateTime(), response.Version(), response.EvictionCriteriaNumber(), response.TTL())
	return entryView, nil
}

func (mp *mapProxy) AddEntryListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	var request *protocol.ClientMessage
	listenerFlags, err := protocol.GetMapListenerFlags(listener)
	if err != nil {
		return nil, err
	}
	request = protocol.MapAddEntryListenerEncodeRequest(mp.name, includeValue, listenerFlags, mp.isSmart())
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, uuid *string,
			numberOfAffectedEntries int32) {
			mp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return mp.client.ListenerService.registerListener(request, eventHandler, func(registrationID *string) *protocol.ClientMessage {
		return protocol.MapRemoveEntryListenerEncodeRequest(mp.name, registrationID)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.MapAddEntryListenerDecodeResponse(clientMessage)()
	})
}

func (mp *mapProxy) AddEntryListenerWithPredicate(listener interface{}, predicate interface{}, includeValue bool) (
	*string, error) {
	var request *protocol.ClientMessage
	listenerFlags, err := protocol.GetMapListenerFlags(listener)
	if err != nil {
		return nil, err
	}
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request = protocol.MapAddEntryListenerWithPredicateEncodeRequest(mp.name, predicateData, includeValue, listenerFlags, false)
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerWithPredicateHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, uuid *string,
			numberOfAffectedEntries int32) {
			mp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return mp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationID *string) *protocol.ClientMessage {
			return protocol.MapRemoveEntryListenerEncodeRequest(mp.name, registrationID)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.MapAddEntryListenerWithPredicateDecodeResponse(clientMessage)()
		})
}

func (mp *mapProxy) AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (
	registrationID *string, err error) {
	var request *protocol.ClientMessage
	listenerFlags, err := protocol.GetMapListenerFlags(listener)
	if err != nil {
		return nil, err
	}
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request = protocol.MapAddEntryListenerToKeyEncodeRequest(mp.name, keyData, includeValue, listenerFlags, mp.isSmart())
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerToKeyHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, uuid *string,
			numberOfAffectedEntries int32) {
			mp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return mp.client.ListenerService.registerListener(request, eventHandler, func(registrationID *string) *protocol.ClientMessage {
		return protocol.MapRemoveEntryListenerEncodeRequest(mp.name, registrationID)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.MapAddEntryListenerToKeyDecodeResponse(clientMessage)()
	})
}

func (mp *mapProxy) AddEntryListenerToKeyWithPredicate(listener interface{}, predicate interface{}, key interface{},
	includeValue bool) (*string, error) {
	var request *protocol.ClientMessage
	listenerFlags, err := protocol.GetMapListenerFlags(listener)
	if err != nil {
		return nil, err
	}
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	predicateData, err := mp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request = protocol.MapAddEntryListenerToKeyWithPredicateEncodeRequest(mp.name, keyData, predicateData, includeValue,
		listenerFlags, false)
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerToKeyWithPredicateHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, uuid *string, numberOfAffectedEntries int32) {
			mp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return mp.client.ListenerService.registerListener(request, eventHandler,
		func(registrationID *string) *protocol.ClientMessage {
			return protocol.MapRemoveEntryListenerEncodeRequest(mp.name, registrationID)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.MapAddEntryListenerToKeyWithPredicateDecodeResponse(clientMessage)()
		})
}

func (mp *mapProxy) onEntryEvent(keyData *serialization.Data, oldValueData *serialization.Data,
	valueData *serialization.Data, mergingValueData *serialization.Data, eventType int32, uuid *string,
	numberOfAffectedEntries int32, includedValue bool, listener interface{}) {
	key, _ := mp.toObject(keyData)
	oldValue, _ := mp.toObject(oldValueData)
	value, _ := mp.toObject(valueData)
	mergingValue, _ := mp.toObject(mergingValueData)
	entryEvent := protocol.NewEntryEvent(key, oldValue, value, mergingValue, eventType, uuid)
	mapEvent := protocol.NewMapEvent(eventType, uuid, numberOfAffectedEntries)
	switch eventType {
	case bufutil.EntryEventAdded:
		listener.(protocol.EntryAddedListener).EntryAdded(entryEvent)
	case bufutil.EntryEventRemoved:
		listener.(protocol.EntryRemovedListener).EntryRemoved(entryEvent)
	case bufutil.EntryEventUpdated:
		listener.(protocol.EntryUpdatedListener).EntryUpdated(entryEvent)
	case bufutil.EntryEventEvicted:
		listener.(protocol.EntryEvictedListener).EntryEvicted(entryEvent)
	case bufutil.EntryEventEvictAll:
		listener.(protocol.EntryEvictAllListener).EntryEvictAll(mapEvent)
	case bufutil.EntryEventClearAll:
		listener.(protocol.EntryClearAllListener).EntryClearAll(mapEvent)
	case bufutil.EntryEventMerged:
		listener.(protocol.EntryMergedListener).EntryMerged(entryEvent)
	case bufutil.EntryEventExpired:
		listener.(protocol.EntryExpiredListener).EntryExpired(entryEvent)
	}
}

func (mp *mapProxy) RemoveEntryListener(registrationID *string) (bool, error) {
	return mp.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *protocol.ClientMessage {
		return protocol.MapRemoveEntryListenerEncodeRequest(mp.name, registrationID)
	})
}

func (mp *mapProxy) ExecuteOnKey(key interface{}, entryProcessor interface{}) (result interface{}, err error) {
	keyData, entryProcessorData, err := mp.validateAndSerialize2(key, entryProcessor)
	if err != nil {
		return nil, err
	}
	request := protocol.MapExecuteOnKeyEncodeRequest(mp.name, entryProcessorData, keyData, threadID)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return mp.decodeToObjectAndError(responseMessage, err, protocol.MapExecuteOnKeyDecodeResponse)
}

func (mp *mapProxy) ExecuteOnKeys(keys []interface{}, entryProcessor interface{}) (keyToResultPairs []core.Pair, err error) {
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

func (mp *mapProxy) ExecuteOnEntries(entryProcessor interface{}) (keyToResultPairs []core.Pair, err error) {
	entryProcessorData, err := mp.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := protocol.MapExecuteOnAllKeysEncodeRequest(mp.name, entryProcessorData)
	responseMessage, err := mp.invokeOnRandomTarget(request)
	return mp.decodeToPairSliceAndError(responseMessage, err, protocol.MapExecuteOnAllKeysDecodeResponse)
}

func (mp *mapProxy) ExecuteOnEntriesWithPredicate(entryProcessor interface{},
	predicate interface{}) (keyToResultPairs []core.Pair, err error) {
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
