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
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"time"
)

type MapProxy struct {
	*proxy
}

func newMapProxy(client *HazelcastClient, serviceName *string, name *string) (*MapProxy, error) {
	return &MapProxy{&proxy{client, serviceName, name}}, nil
}

func (imap *MapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := protocol.MapPutEncodeRequest(imap.name, keyData, valueData, threadId, ttlUnlimited)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToObjectAndError(responseMessage, err, protocol.MapPutDecodeResponse)
}
func (imap *MapProxy) TryPut(key interface{}, value interface{}) (ok bool, err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := protocol.MapTryPutEncodeRequest(imap.name, keyData, valueData, threadId, ttlUnlimited)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToBoolAndError(responseMessage, err, protocol.MapTryPutDecodeResponse)
}
func (imap *MapProxy) PutTransient(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) (err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	ttl = common.GetTimeInMilliSeconds(ttl, ttlTimeUnit)
	request := protocol.MapPutTransientEncodeRequest(imap.name, keyData, valueData, threadId, ttl)
	_, err = imap.invokeOnKey(request, keyData)
	return err
}
func (imap *MapProxy) Get(key interface{}) (value interface{}, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.MapGetEncodeRequest(imap.name, keyData, threadId)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToObjectAndError(responseMessage, err, protocol.MapGetDecodeResponse)

}
func (imap *MapProxy) Remove(key interface{}) (value interface{}, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.MapRemoveEncodeRequest(imap.name, keyData, threadId)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToObjectAndError(responseMessage, err, protocol.MapRemoveDecodeResponse)

}
func (imap *MapProxy) RemoveIfSame(key interface{}, value interface{}) (ok bool, err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return false, err
	}
	request := protocol.MapRemoveIfSameEncodeRequest(imap.name, keyData, valueData, threadId)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToBoolAndError(responseMessage, err, protocol.MapRemoveIfSameDecodeResponse)

}
func (imap *MapProxy) RemoveAll(predicate interface{}) (err error) {
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return err
	}
	request := protocol.MapRemoveAllEncodeRequest(imap.name, predicateData)
	_, err = imap.invokeOnRandomTarget(request)
	return err
}
func (imap *MapProxy) TryRemove(key interface{}, timeout int64, timeoutTimeUnit time.Duration) (ok bool, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	timeout = common.GetTimeInMilliSeconds(timeout, timeoutTimeUnit)
	request := protocol.MapTryRemoveEncodeRequest(imap.name, keyData, threadId, timeout)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToBoolAndError(responseMessage, err, protocol.MapTryRemoveDecodeResponse)
}
func (imap *MapProxy) Size() (size int32, err error) {
	request := protocol.MapSizeEncodeRequest(imap.name)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToInt32AndError(responseMessage, err, protocol.MapSizeDecodeResponse)

}
func (imap *MapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.MapContainsKeyEncodeRequest(imap.name, keyData, threadId)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToBoolAndError(responseMessage, err, protocol.MapContainsKeyDecodeResponse)

}
func (imap *MapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := imap.validateAndSerialize(value)
	if err != nil {
		return false, err
	}
	request := protocol.MapContainsValueEncodeRequest(imap.name, valueData)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToBoolAndError(responseMessage, err, protocol.MapContainsValueDecodeResponse)

}
func (imap *MapProxy) Clear() (err error) {
	request := protocol.MapClearEncodeRequest(imap.name)
	_, err = imap.invokeOnRandomTarget(request)
	return
}
func (imap *MapProxy) Delete(key interface{}) (err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MapDeleteEncodeRequest(imap.name, keyData, threadId)
	_, err = imap.invokeOnKey(request, keyData)
	return
}
func (imap *MapProxy) IsEmpty() (empty bool, err error) {
	request := protocol.MapIsEmptyEncodeRequest(imap.name)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToBoolAndError(responseMessage, err, protocol.MapIsEmptyDecodeResponse)

}
func (imap *MapProxy) AddIndex(attribute string, ordered bool) (err error) {
	request := protocol.MapAddIndexEncodeRequest(imap.name, &attribute, ordered)
	_, err = imap.invokeOnRandomTarget(request)
	return
}
func (imap *MapProxy) Evict(key interface{}) (evicted bool, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.MapEvictEncodeRequest(imap.name, keyData, threadId)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToBoolAndError(responseMessage, err, protocol.MapEvictDecodeResponse)

}
func (imap *MapProxy) EvictAll() (err error) {
	request := protocol.MapEvictAllEncodeRequest(imap.name)
	_, err = imap.invokeOnRandomTarget(request)
	return
}
func (imap *MapProxy) Flush() (err error) {
	request := protocol.MapFlushEncodeRequest(imap.name)
	_, err = imap.invokeOnRandomTarget(request)
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
	lease = common.GetTimeInMilliSeconds(lease, leaseTimeUnit)
	request := protocol.MapLockEncodeRequest(imap.name, keyData, threadId, lease, imap.client.ProxyManager.nextReferenceId())
	_, err = imap.invokeOnKey(request, keyData)
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
	timeout = common.GetTimeInMilliSeconds(timeout, timeoutTimeUnit)
	lease = common.GetTimeInMilliSeconds(lease, leaseTimeUnit)
	request := protocol.MapTryLockEncodeRequest(imap.name, keyData, threadId, lease, timeout, imap.client.ProxyManager.nextReferenceId())
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToBoolAndError(responseMessage, err, protocol.MapTryLockDecodeResponse)
}
func (imap *MapProxy) Unlock(key interface{}) (err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MapUnlockEncodeRequest(imap.name, keyData, threadId, imap.client.ProxyManager.nextReferenceId())
	_, err = imap.invokeOnKey(request, keyData)
	return
}
func (imap *MapProxy) ForceUnlock(key interface{}) (err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MapForceUnlockEncodeRequest(imap.name, keyData, imap.client.ProxyManager.nextReferenceId())
	_, err = imap.invokeOnKey(request, keyData)
	return
}
func (imap *MapProxy) IsLocked(key interface{}) (locked bool, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.MapIsLockedEncodeRequest(imap.name, keyData)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToBoolAndError(responseMessage, err, protocol.MapIsLockedDecodeResponse)

}
func (imap *MapProxy) Replace(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := protocol.MapReplaceEncodeRequest(imap.name, keyData, valueData, threadId)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToObjectAndError(responseMessage, err, protocol.MapReplaceDecodeResponse)

}
func (imap *MapProxy) ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (replaced bool, err error) {
	keyData, oldValueData, newValueData, err := imap.validateAndSerialize3(key, oldValue, newValue)
	if err != nil {
		return false, err
	}
	request := protocol.MapReplaceIfSameEncodeRequest(imap.name, keyData, oldValueData, newValueData, threadId)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToBoolAndError(responseMessage, err, protocol.MapReplaceIfSameDecodeResponse)

}
func (imap *MapProxy) Set(key interface{}, value interface{}) (err error) {
	return imap.SetWithTtl(key, value, ttlUnlimited, time.Second)
}
func (imap *MapProxy) SetWithTtl(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) (err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return err
	}
	ttl = common.GetTimeInMilliSeconds(ttl, ttlTimeUnit)
	request := protocol.MapSetEncodeRequest(imap.name, keyData, valueData, threadId, ttl)
	_, err = imap.invokeOnKey(request, keyData)
	return
}

func (imap *MapProxy) PutIfAbsent(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := imap.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := protocol.MapPutIfAbsentEncodeRequest(imap.name, keyData, valueData, threadId, ttlUnlimited)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToObjectAndError(responseMessage, err, protocol.MapPutIfAbsentDecodeResponse)

}
func (imap *MapProxy) PutAll(entries map[interface{}]interface{}) (err error) {
	if entries == nil {
		return core.NewHazelcastNilPointerError(common.NilMapIsNotAllowed, nil)
	}
	partitions, err := imap.validateAndSerializeMapAndGetPartitions(entries)
	if err != nil {
		return err
	}
	for partitionId, entryList := range partitions {
		request := protocol.MapPutAllEncodeRequest(imap.name, entryList)
		_, err = imap.invokeOnPartition(request, partitionId)
		if err != nil {
			return err
		}
	}
	return nil
}
func (imap *MapProxy) KeySet() (keySet []interface{}, err error) {
	request := protocol.MapKeySetEncodeRequest(imap.name)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapKeySetDecodeResponse)
}
func (imap *MapProxy) KeySetWithPredicate(predicate interface{}) (keySet []interface{}, err error) {
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := protocol.MapKeySetWithPredicateEncodeRequest(imap.name, predicateData)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapKeySetWithPredicateDecodeResponse)
}
func (imap *MapProxy) Values() (values []interface{}, err error) {
	request := protocol.MapValuesEncodeRequest(imap.name)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapValuesDecodeResponse)
}
func (imap *MapProxy) ValuesWithPredicate(predicate interface{}) (values []interface{}, err error) {
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := protocol.MapValuesWithPredicateEncodeRequest(imap.name, predicateData)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MapValuesWithPredicateDecodeResponse)
}
func (imap *MapProxy) EntrySet() (resultPairs []core.IPair, err error) {
	request := protocol.MapEntrySetEncodeRequest(imap.name)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToPairSliceAndError(responseMessage, err, protocol.MapEntrySetDecodeResponse)
}
func (imap *MapProxy) EntrySetWithPredicate(predicate interface{}) (resultPairs []core.IPair, err error) {
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := protocol.MapEntriesWithPredicateEncodeRequest(imap.name, predicateData)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToPairSliceAndError(responseMessage, err, protocol.MapEntriesWithPredicateDecodeResponse)
}
func (imap *MapProxy) GetAll(keys []interface{}) (entryMap map[interface{}]interface{}, err error) {
	if keys == nil {
		return nil, core.NewHazelcastNilPointerError(common.NilKeysAreNotAllowed, nil)
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
		request := protocol.MapGetAllEncodeRequest(imap.name, keyList)
		responseMessage, err := imap.invokeOnPartition(request, partitionId)
		if err != nil {
			return nil, err
		}
		response := protocol.MapGetAllDecodeResponse(responseMessage)()
		for _, pairData := range response {
			key, err := imap.toObject(pairData.Key().(*serialization.Data))
			if err != nil {
				return nil, err
			}
			value, err := imap.toObject(pairData.Value().(*serialization.Data))
			if err != nil {
				return nil, err
			}
			entryMap[key] = value
		}
	}
	return entryMap, nil
}
func (imap *MapProxy) GetEntryView(key interface{}) (entryView core.IEntryView, err error) {
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.MapGetEntryViewEncodeRequest(imap.name, keyData, threadId)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	response := protocol.MapGetEntryViewDecodeResponse(responseMessage)()
	resultKey, _ := imap.toObject(response.KeyData())
	resultValue, _ := imap.toObject(response.ValueData())
	entryView = protocol.NewEntryView(resultKey, resultValue, response.Cost(),
		response.CreationTime(), response.ExpirationTime(), response.Hits(), response.LastAccessTime(), response.LastStoredTime(),
		response.LastUpdateTime(), response.Version(), response.EvictionCriteriaNumber(), response.Ttl())
	return entryView, nil
}
func (imap *MapProxy) AddEntryListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	var request *protocol.ClientMessage
	listenerFlags := protocol.GetEntryListenerFlags(listener)
	request = protocol.MapAddEntryListenerEncodeRequest(imap.name, includeValue, listenerFlags, imap.isSmart())
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *protocol.ClientMessage {
		return protocol.MapRemoveEntryListenerEncodeRequest(imap.name, registrationId)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.MapAddEntryListenerDecodeResponse(clientMessage)()
	})
}
func (imap *MapProxy) AddEntryListenerWithPredicate(listener interface{}, predicate interface{}, includeValue bool) (*string, error) {
	var request *protocol.ClientMessage
	listenerFlags := protocol.GetEntryListenerFlags(listener)
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request = protocol.MapAddEntryListenerWithPredicateEncodeRequest(imap.name, predicateData, includeValue, listenerFlags, false)
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerWithPredicateHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *protocol.ClientMessage {
			return protocol.MapRemoveEntryListenerEncodeRequest(imap.name, registrationId)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.MapAddEntryListenerWithPredicateDecodeResponse(clientMessage)()
		})
}
func (imap *MapProxy) AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID *string, err error) {
	var request *protocol.ClientMessage
	listenerFlags := protocol.GetEntryListenerFlags(listener)
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request = protocol.MapAddEntryListenerToKeyEncodeRequest(imap.name, keyData, includeValue, listenerFlags, imap.isSmart())
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerToKeyHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *protocol.ClientMessage {
		return protocol.MapRemoveEntryListenerEncodeRequest(imap.name, registrationId)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.MapAddEntryListenerToKeyDecodeResponse(clientMessage)()
	})
}
func (imap *MapProxy) AddEntryListenerToKeyWithPredicate(listener interface{}, predicate interface{}, key interface{}, includeValue bool) (*string, error) {
	var request *protocol.ClientMessage
	listenerFlags := protocol.GetEntryListenerFlags(listener)
	keyData, err := imap.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request = protocol.MapAddEntryListenerToKeyWithPredicateEncodeRequest(imap.name, keyData, predicateData, includeValue, listenerFlags, false)
	eventHandler := func(clientMessage *protocol.ClientMessage) {
		protocol.MapAddEntryListenerToKeyWithPredicateHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *protocol.ClientMessage {
			return protocol.MapRemoveEntryListenerEncodeRequest(imap.name, registrationId)
		}, func(clientMessage *protocol.ClientMessage) *string {
			return protocol.MapAddEntryListenerToKeyWithPredicateDecodeResponse(clientMessage)()
		})
}
func (imap *MapProxy) onEntryEvent(keyData *serialization.Data, oldValueData *serialization.Data, valueData *serialization.Data, mergingValueData *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32, includedValue bool, listener interface{}) {
	key, _ := imap.toObject(keyData)
	oldValue, _ := imap.toObject(oldValueData)
	value, _ := imap.toObject(valueData)
	mergingValue, _ := imap.toObject(mergingValueData)
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

func (imap *MapProxy) RemoveEntryListener(registrationId *string) (bool, error) {
	return imap.client.ListenerService.deregisterListener(*registrationId, func(registrationId *string) *protocol.ClientMessage {
		return protocol.MapRemoveEntryListenerEncodeRequest(imap.name, registrationId)
	})
}

func (imap *MapProxy) ExecuteOnKey(key interface{}, entryProcessor interface{}) (result interface{}, err error) {
	keyData, entryProcessorData, err := imap.validateAndSerialize2(key, entryProcessor)
	if err != nil {
		return nil, err
	}
	request := protocol.MapExecuteOnKeyEncodeRequest(imap.name, entryProcessorData, keyData, threadId)
	responseMessage, err := imap.invokeOnKey(request, keyData)
	return imap.decodeToObjectAndError(responseMessage, err, protocol.MapExecuteOnKeyDecodeResponse)
}
func (imap *MapProxy) ExecuteOnKeys(keys []interface{}, entryProcessor interface{}) (keyToResultPairs []core.IPair, err error) {
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
	request := protocol.MapExecuteOnKeysEncodeRequest(imap.name, entryProcessorData, keysData)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToPairSliceAndError(responseMessage, err, protocol.MapExecuteOnKeysDecodeResponse)
}
func (imap *MapProxy) ExecuteOnEntries(entryProcessor interface{}) (keyToResultPairs []core.IPair, err error) {
	entryProcessorData, err := imap.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := protocol.MapExecuteOnAllKeysEncodeRequest(imap.name, entryProcessorData)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToPairSliceAndError(responseMessage, err, protocol.MapExecuteOnAllKeysDecodeResponse)
}

func (imap *MapProxy) ExecuteOnEntriesWithPredicate(entryProcessor interface{}, predicate interface{}) (keyToResultPairs []core.IPair, err error) {
	predicateData, err := imap.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	entryProcessorData, err := imap.validateAndSerialize(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := protocol.MapExecuteWithPredicateEncodeRequest(imap.name, entryProcessorData, predicateData)
	responseMessage, err := imap.invokeOnRandomTarget(request)
	return imap.decodeToPairSliceAndError(responseMessage, err, protocol.MapExecuteWithPredicateDecodeResponse)
}
