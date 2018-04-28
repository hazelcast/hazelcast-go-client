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
	"math/rand"
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type replicatedMapProxy struct {
	*proxy
	tarGetPartitionID int32
}

func newReplicatedMapProxy(client *HazelcastClient, serviceName *string, name *string) (*replicatedMapProxy, error) {
	partitionCount := client.PartitionService.getPartitionCount()
	tarGetPartitionID := rand.Int31n(partitionCount)
	return &replicatedMapProxy{proxy: &proxy{client, serviceName, name}, tarGetPartitionID: tarGetPartitionID}, nil
}

func (rmp *replicatedMapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	return rmp.PutWithTTL(key, value, ttlUnlimited)
}

func (rmp *replicatedMapProxy) PutWithTTL(key interface{}, value interface{},
	ttl time.Duration) (oldValue interface{}, err error) {
	keyData, valueData, err := rmp.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	ttlInMillis := bufutil.GetTimeInMilliSeconds(ttl)
	request := protocol.ReplicatedMapPutEncodeRequest(rmp.name, keyData, valueData, ttlInMillis)
	responseMessage, err := rmp.invokeOnKey(request, keyData)
	return rmp.decodeToObjectAndError(responseMessage, err, protocol.ReplicatedMapPutDecodeResponse)
}

func (rmp *replicatedMapProxy) PutAll(entries map[interface{}]interface{}) (err error) {
	if entries == nil {
		return core.NewHazelcastNilPointerError(bufutil.NilMapIsNotAllowed, nil)
	}
	pairs := make([]*protocol.Pair, len(entries))
	index := 0
	for key, value := range entries {
		keyData, valueData, err := rmp.validateAndSerialize2(key, value)
		if err != nil {
			return err
		}
		pairs[index] = protocol.NewPair(keyData, valueData)
		index++
	}
	request := protocol.ReplicatedMapPutAllEncodeRequest(rmp.name, pairs)
	_, err = rmp.invokeOnRandomTarget(request)
	return err
}

func (rmp *replicatedMapProxy) Get(key interface{}) (value interface{}, err error) {
	keyData, err := rmp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.ReplicatedMapGetEncodeRequest(rmp.name, keyData)
	responseMessage, err := rmp.invokeOnKey(request, keyData)
	return rmp.decodeToObjectAndError(responseMessage, err, protocol.ReplicatedMapGetDecodeResponse)
}

func (rmp *replicatedMapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := rmp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.ReplicatedMapContainsKeyEncodeRequest(rmp.name, keyData)
	responseMessage, err := rmp.invokeOnKey(request, keyData)
	return rmp.decodeToBoolAndError(responseMessage, err, protocol.ReplicatedMapContainsKeyDecodeResponse)
}

func (rmp *replicatedMapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := rmp.validateAndSerialize(value)
	if err != nil {
		return false, err
	}
	request := protocol.ReplicatedMapContainsValueEncodeRequest(rmp.name, valueData)
	responseMessage, err := rmp.invokeOnKey(request, valueData)
	return rmp.decodeToBoolAndError(responseMessage, err, protocol.ReplicatedMapContainsValueDecodeResponse)
}

func (rmp *replicatedMapProxy) Clear() (err error) {
	request := protocol.ReplicatedMapClearEncodeRequest(rmp.name)
	_, err = rmp.invokeOnRandomTarget(request)
	return err
}

func (rmp *replicatedMapProxy) Remove(key interface{}) (value interface{}, err error) {
	keyData, err := rmp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.ReplicatedMapRemoveEncodeRequest(rmp.name, keyData)
	responseMessage, err := rmp.invokeOnKey(request, keyData)
	return rmp.decodeToObjectAndError(responseMessage, err, protocol.ReplicatedMapRemoveDecodeResponse)
}

func (rmp *replicatedMapProxy) IsEmpty() (empty bool, err error) {
	request := protocol.ReplicatedMapIsEmptyEncodeRequest(rmp.name)
	responseMessage, err := rmp.invokeOnPartition(request, rmp.tarGetPartitionID)
	return rmp.decodeToBoolAndError(responseMessage, err, protocol.ReplicatedMapIsEmptyDecodeResponse)
}

func (rmp *replicatedMapProxy) Size() (size int32, err error) {
	request := protocol.ReplicatedMapSizeEncodeRequest(rmp.name)
	responseMessage, err := rmp.invokeOnPartition(request, rmp.tarGetPartitionID)
	return rmp.decodeToInt32AndError(responseMessage, err, protocol.ReplicatedMapSizeDecodeResponse)

}

func (rmp *replicatedMapProxy) Values() (values []interface{}, err error) {
	request := protocol.ReplicatedMapValuesEncodeRequest(rmp.name)
	responseMessage, err := rmp.invokeOnPartition(request, rmp.tarGetPartitionID)
	return rmp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.ReplicatedMapValuesDecodeResponse)
}

func (rmp *replicatedMapProxy) KeySet() (keySet []interface{}, err error) {
	request := protocol.ReplicatedMapKeySetEncodeRequest(rmp.name)
	responseMessage, err := rmp.invokeOnPartition(request, rmp.tarGetPartitionID)
	return rmp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.ReplicatedMapKeySetDecodeResponse)
}

func (rmp *replicatedMapProxy) EntrySet() (resultPairs []core.Pair, err error) {
	request := protocol.ReplicatedMapEntrySetEncodeRequest(rmp.name)
	responseMessage, err := rmp.invokeOnPartition(request, rmp.tarGetPartitionID)
	return rmp.decodeToPairSliceAndError(responseMessage, err, protocol.ReplicatedMapEntrySetDecodeResponse)
}

func (rmp *replicatedMapProxy) AddEntryListener(listener interface{}) (registrationID *string, err error) {
	request := protocol.ReplicatedMapAddEntryListenerEncodeRequest(rmp.name, rmp.isSmart())
	eventHandler := rmp.createEventHandler(listener)
	return rmp.client.ListenerService.registerListener(request, eventHandler, func(registrationID *string) *protocol.ClientMessage {
		return protocol.ReplicatedMapRemoveEntryListenerEncodeRequest(rmp.name, registrationID)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.ReplicatedMapAddEntryListenerDecodeResponse(clientMessage)()
	})
}

func (rmp *replicatedMapProxy) AddEntryListenerWithPredicate(listener interface{},
	predicate interface{}) (registrationID *string, err error) {
	predicateData, err := rmp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	request := protocol.ReplicatedMapAddEntryListenerWithPredicateEncodeRequest(rmp.name, predicateData, rmp.isSmart())
	eventHandler := rmp.createEventHandlerWithPredicate(listener)
	return rmp.client.ListenerService.registerListener(request, eventHandler, func(registrationID *string) *protocol.ClientMessage {
		return protocol.ReplicatedMapRemoveEntryListenerEncodeRequest(rmp.name, registrationID)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.ReplicatedMapAddEntryListenerWithPredicateDecodeResponse(clientMessage)()
	})
}

func (rmp *replicatedMapProxy) AddEntryListenerToKey(listener interface{}, key interface{}) (registrationID *string, err error) {
	keyData, err := rmp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.ReplicatedMapAddEntryListenerToKeyEncodeRequest(rmp.name, keyData, rmp.isSmart())
	eventHandler := rmp.createEventHandlerToKey(listener)
	return rmp.client.ListenerService.registerListener(request, eventHandler, func(registrationID *string) *protocol.ClientMessage {
		return protocol.ReplicatedMapRemoveEntryListenerEncodeRequest(rmp.name, registrationID)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.ReplicatedMapAddEntryListenerToKeyDecodeResponse(clientMessage)()
	})
}

func (rmp *replicatedMapProxy) AddEntryListenerToKeyWithPredicate(listener interface{}, predicate interface{},
	key interface{}) (registrationID *string, err error) {
	predicateData, err := rmp.validateAndSerializePredicate(predicate)
	if err != nil {
		return nil, err
	}
	keyData, err := rmp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.ReplicatedMapAddEntryListenerToKeyWithPredicateEncodeRequest(rmp.name, keyData, predicateData, rmp.isSmart())
	eventHandler := rmp.createEventHandlerToKeyWithPredicate(listener)
	return rmp.client.ListenerService.registerListener(request, eventHandler, func(registrationID *string) *protocol.ClientMessage {
		return protocol.ReplicatedMapRemoveEntryListenerEncodeRequest(rmp.name, registrationID)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.ReplicatedMapAddEntryListenerToKeyWithPredicateDecodeResponse(clientMessage)()
	})
}

func (rmp *replicatedMapProxy) RemoveEntryListener(registrationID *string) (removed bool, err error) {
	return rmp.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *protocol.ClientMessage {
		return protocol.ReplicatedMapRemoveEntryListenerEncodeRequest(rmp.name, registrationID)
	})
}

func (rmp *replicatedMapProxy) onEntryEvent(keyData *serialization.Data, oldValueData *serialization.Data,
	valueData *serialization.Data, mergingValueData *serialization.Data, eventType int32, uuid *string,
	numberOfAffectedEntries int32, listener interface{}) {
	key, _ := rmp.toObject(keyData)
	oldValue, _ := rmp.toObject(oldValueData)
	value, _ := rmp.toObject(valueData)
	mergingValue, _ := rmp.toObject(mergingValueData)
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
	case bufutil.EntryEventClearAll:
		listener.(protocol.EntryClearAllListener).EntryClearAll(mapEvent)
	}
}

func (rmp *replicatedMapProxy) createEventHandler(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.ReplicatedMapAddEntryListenerHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, uuid *string, numberOfAffectedEntries int32) {
			rmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, listener)
		})
	}
}

func (rmp *replicatedMapProxy) createEventHandlerWithPredicate(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.ReplicatedMapAddEntryListenerWithPredicateHandle(clientMessage,
			func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data,
				eventType int32, uuid *string, numberOfAffectedEntries int32) {
				rmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, listener)
			})
	}
}

func (rmp *replicatedMapProxy) createEventHandlerToKey(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.ReplicatedMapAddEntryListenerToKeyHandle(clientMessage,
			func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data,
				eventType int32, uuid *string, numberOfAffectedEntries int32) {
				rmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, listener)
			})
	}
}

func (rmp *replicatedMapProxy) createEventHandlerToKeyWithPredicate(listener interface{}) func(
	clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.ReplicatedMapAddEntryListenerToKeyWithPredicateHandle(clientMessage,
			func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data,
				eventType int32, uuid *string, numberOfAffectedEntries int32) {
				rmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, listener)
			})
	}
}
