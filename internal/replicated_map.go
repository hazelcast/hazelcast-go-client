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
	"math/rand"
	"time"
)

type ReplicatedMapProxy struct {
	*proxy
	targetPartitionId int32
}

func newReplicatedMapProxy(client *HazelcastClient, serviceName *string, name *string) *ReplicatedMapProxy {
	partitionCount := client.PartitionService.getPartitionCount()
	targetPartitionId := rand.Int31n(partitionCount)
	return &ReplicatedMapProxy{proxy: &proxy{client, serviceName, name}, targetPartitionId: targetPartitionId}
}
func (rmp *ReplicatedMapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	return rmp.PutWithTtl(key, value, ttlUnlimited, time.Second)
}

func (rmp *ReplicatedMapProxy) PutWithTtl(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) (oldValue interface{}, err error) {
	keyData, valueData, err := rmp.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := ReplicatedMapPutEncodeRequest(rmp.name, keyData, valueData, ttl)
	responseMessage, err := rmp.invokeOnKey(request, keyData)
	return rmp.decodeToObjectAndError(responseMessage, err, ReplicatedMapPutDecodeResponse)
}

func (rmp *ReplicatedMapProxy) PutAll(entries map[interface{}]interface{}) (err error) {
	if entries == nil {
		return NewHazelcastNilPointerError(NIL_MAP_IS_NOT_ALLOWED, nil)
	}
	pairs := make([]*Pair, len(entries))
	index := 0
	for key, value := range entries {
		keyData, valueData, err := rmp.validateAndSerialize2(key, value)
		if err != nil {
			return err
		}
		pairs[index] = NewPair(keyData, valueData)
		index++
	}
	request := ReplicatedMapPutAllEncodeRequest(rmp.name, pairs)
	_, err = rmp.invokeOnRandomTarget(request)
	return err
}

func (rmp *ReplicatedMapProxy) Get(key interface{}) (value interface{}, err error) {
	keyData, err := rmp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := ReplicatedMapGetEncodeRequest(rmp.name, keyData)
	responseMessage, err := rmp.invokeOnKey(request, keyData)
	return rmp.decodeToObjectAndError(responseMessage, err, ReplicatedMapGetDecodeResponse)
}

func (rmp *ReplicatedMapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := rmp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := ReplicatedMapContainsKeyEncodeRequest(rmp.name, keyData)
	responseMessage, err := rmp.invokeOnKey(request, keyData)
	return rmp.decodeToBoolAndError(responseMessage, err, ReplicatedMapContainsKeyDecodeResponse)
}

func (rmp *ReplicatedMapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := rmp.validateAndSerialize(value)
	if err != nil {
		return false, err
	}
	request := ReplicatedMapContainsValueEncodeRequest(rmp.name, valueData)
	responseMessage, err := rmp.invokeOnKey(request, valueData)
	return rmp.decodeToBoolAndError(responseMessage, err, ReplicatedMapContainsValueDecodeResponse)
}

func (rmp *ReplicatedMapProxy) Clear() (err error) {
	request := ReplicatedMapClearEncodeRequest(rmp.name)
	_, err = rmp.invokeOnRandomTarget(request)
	return err
}

func (rmp *ReplicatedMapProxy) Remove(key interface{}) (value interface{}, err error) {
	keyData, err := rmp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := ReplicatedMapRemoveEncodeRequest(rmp.name, keyData)
	responseMessage, err := rmp.invokeOnKey(request, keyData)
	return rmp.decodeToObjectAndError(responseMessage, err, ReplicatedMapRemoveDecodeResponse)
}

func (rmp *ReplicatedMapProxy) IsEmpty() (empty bool, err error) {
	request := ReplicatedMapIsEmptyEncodeRequest(rmp.name)
	responseMessage, err := rmp.invokeOnPartition(request, rmp.targetPartitionId)
	return rmp.decodeToBoolAndError(responseMessage, err, ReplicatedMapIsEmptyDecodeResponse)
}

func (rmp *ReplicatedMapProxy) Size() (size int32, err error) {
	request := ReplicatedMapSizeEncodeRequest(rmp.name)
	responseMessage, err := rmp.invokeOnPartition(request, rmp.targetPartitionId)
	return rmp.decodeToInt32AndError(responseMessage, err, ReplicatedMapSizeDecodeResponse)

}

func (rmp *ReplicatedMapProxy) Values() (values []interface{}, err error) {
	request := ReplicatedMapValuesEncodeRequest(rmp.name)
	responseMessage, err := rmp.invokeOnPartition(request, rmp.targetPartitionId)
	return rmp.decodeToInterfaceSliceAndError(responseMessage, err, ReplicatedMapValuesDecodeResponse)
}

func (rmp *ReplicatedMapProxy) KeySet() (keySet []interface{}, err error) {
	request := ReplicatedMapKeySetEncodeRequest(rmp.name)
	responseMessage, err := rmp.invokeOnPartition(request, rmp.targetPartitionId)
	return rmp.decodeToInterfaceSliceAndError(responseMessage, err, ReplicatedMapKeySetDecodeResponse)
}

func (rmp *ReplicatedMapProxy) EntrySet() (resultPairs []IPair, err error) {
	request := ReplicatedMapEntrySetEncodeRequest(rmp.name)
	responseMessage, err := rmp.invokeOnPartition(request, rmp.targetPartitionId)
	return rmp.decodeToPairSliceAndError(responseMessage, err, ReplicatedMapEntrySetDecodeResponse)
}

func (rmp *ReplicatedMapProxy) AddEntryListener(listener interface{}) (registrationID *string, err error) {
	request := ReplicatedMapAddEntryListenerEncodeRequest(rmp.name, rmp.isSmart())
	eventHandler := rmp.createEventHandler(listener)
	return rmp.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *ClientMessage {
		return ReplicatedMapRemoveEntryListenerEncodeRequest(rmp.name, registrationId)
	}, func(clientMessage *ClientMessage) *string {
		return ReplicatedMapAddEntryListenerDecodeResponse(clientMessage)()
	})
}

func (rmp *ReplicatedMapProxy) AddEntryListenerWithPredicate(listener interface{}, predicate interface{}) (registrationID *string, err error) {
	predicateData, err := rmp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := ReplicatedMapAddEntryListenerWithPredicateEncodeRequest(rmp.name, predicateData, rmp.isSmart())
	eventHandler := rmp.createEventHandlerWithPredicate(listener)
	return rmp.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *ClientMessage {
		return ReplicatedMapRemoveEntryListenerEncodeRequest(rmp.name, registrationId)
	}, func(clientMessage *ClientMessage) *string {
		return ReplicatedMapAddEntryListenerWithPredicateDecodeResponse(clientMessage)()
	})
}

func (rmp *ReplicatedMapProxy) AddEntryListenerToKey(listener interface{}, key interface{}) (registrationID *string, err error) {
	keyData, err := rmp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := ReplicatedMapAddEntryListenerToKeyEncodeRequest(rmp.name, keyData, rmp.isSmart())
	eventHandler := rmp.createEventHandlerToKey(listener)
	return rmp.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *ClientMessage {
		return ReplicatedMapRemoveEntryListenerEncodeRequest(rmp.name, registrationId)
	}, func(clientMessage *ClientMessage) *string {
		return ReplicatedMapAddEntryListenerToKeyDecodeResponse(clientMessage)()
	})
}

func (rmp *ReplicatedMapProxy) AddEntryListenerToKeyWithPredicate(listener interface{}, predicate interface{}, key interface{}) (registrationID *string, err error) {
	predicateData, err := rmp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	keyData, err := rmp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := ReplicatedMapAddEntryListenerToKeyWithPredicateEncodeRequest(rmp.name, keyData, predicateData, rmp.isSmart())
	eventHandler := rmp.createEventHandlerToKeyWithPredicate(listener)
	return rmp.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *ClientMessage {
		return ReplicatedMapRemoveEntryListenerEncodeRequest(rmp.name, registrationId)
	}, func(clientMessage *ClientMessage) *string {
		return ReplicatedMapAddEntryListenerToKeyWithPredicateDecodeResponse(clientMessage)()
	})
}

func (rmp *ReplicatedMapProxy) RemoveEntryListener(registrationId *string) (removed bool, err error) {
	return rmp.client.ListenerService.deregisterListener(*registrationId, func(registrationId *string) *ClientMessage {
		return ReplicatedMapRemoveEntryListenerEncodeRequest(rmp.name, registrationId)
	})
}

func (rmp *ReplicatedMapProxy) onEntryEvent(keyData *serialization.Data, oldValueData *serialization.Data, valueData *serialization.Data, mergingValueData *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32, listener interface{}) {
	key, _ := rmp.toObject(keyData)
	oldValue, _ := rmp.toObject(oldValueData)
	value, _ := rmp.toObject(valueData)
	mergingValue, _ := rmp.toObject(mergingValueData)
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
	case ENTRYEVENT_CLEAR_ALL:
		listener.(EntryClearAllListener).EntryClearAll(mapEvent)
	}
}

func (rmp *ReplicatedMapProxy) createEventHandler(listener interface{}) func(clientMessage *ClientMessage) {
	return func(clientMessage *ClientMessage) {
		ReplicatedMapAddEntryListenerHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			rmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, listener)
		})
	}
}

func (rmp *ReplicatedMapProxy) createEventHandlerWithPredicate(listener interface{}) func(clientMessage *ClientMessage) {
	return func(clientMessage *ClientMessage) {
		ReplicatedMapAddEntryListenerWithPredicateHandle(clientMessage,
			func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data,
				eventType int32, Uuid *string, numberOfAffectedEntries int32) {
				rmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, listener)
			})
	}
}

func (rmp *ReplicatedMapProxy) createEventHandlerToKey(listener interface{}) func(clientMessage *ClientMessage) {
	return func(clientMessage *ClientMessage) {
		ReplicatedMapAddEntryListenerToKeyHandle(clientMessage,
			func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data,
				eventType int32, Uuid *string, numberOfAffectedEntries int32) {
				rmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, listener)
			})
	}
}

func (rmp *ReplicatedMapProxy) createEventHandlerToKeyWithPredicate(listener interface{}) func(clientMessage *ClientMessage) {
	return func(clientMessage *ClientMessage) {
		ReplicatedMapAddEntryListenerToKeyWithPredicateHandle(clientMessage,
			func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data,
				eventType int32, Uuid *string, numberOfAffectedEntries int32) {
				rmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, listener)
			})
	}
}
