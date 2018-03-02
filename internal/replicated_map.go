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
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	. "github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type ReplicatedMapProxy struct {
	*partitionSpecificProxy
}

func newReplicatedMapProxy(client *HazelcastClient, serviceName *string, name *string) (*ReplicatedMapProxy, error) {
	parSpecProxy, err := newPartitionSpecificProxy(client, serviceName, name)
	if err != nil {
		return nil, err
	}
	return &ReplicatedMapProxy{parSpecProxy}, nil
}

func (rmp *ReplicatedMapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	if err = AssertNotNil(key, "key"); err != nil {
		return nil, err
	}
	if err = AssertNotNil(key, "value"); err != nil {
		return nil, err
	}
	keyData, err := rmp.ToData(key)
	if err != nil {
		return nil, err
	}
	valueData, err := rmp.ToData(value)
	if err != nil {
		return nil, err
	}

	request := ReplicatedMapPutEncodeRequest(rmp.name, keyData, valueData, TTL)
	responseMessage, err := rmp.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapPutDecodeResponse(responseMessage).Response
	return rmp.ToObject(responseData)
}

func (rmp *ReplicatedMapProxy) PutWithTtl(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) (oldValue interface{}, err error) {
	if err = AssertNotNil(key, "key"); err != nil {
		return nil, err
	}
	if err = AssertNotNil(key, "value"); err != nil {
		return nil, err
	}
	keyData, err := rmp.ToData(key)
	if err != nil {
		return nil, err
	}
	valueData, err := rmp.ToData(value)
	if err != nil {
		return nil, err
	}
	request := ReplicatedMapPutEncodeRequest(rmp.name, keyData, valueData, ttl)
	responseMessage, err := rmp.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapPutDecodeResponse(responseMessage).Response
	return rmp.ToObject(responseData)
}

func (rmp *ReplicatedMapProxy) PutAll(entries map[interface{}]interface{}) (err error) {
	if err = AssertNotNil(entries, "entries"); err != nil {
		return err
	}
	partitions := make(map[int32][]Pair)
	for key, value := range entries {
		keyData, err := rmp.ToData(key)
		if err != nil {
			return err
		}
		valueData, err := rmp.ToData(value)
		if err != nil {
			return err
		}
		pair := NewPair(keyData, valueData)

		partitionId := rmp.client.PartitionService.GetPartitionId(keyData)
		partitions[partitionId] = append(partitions[partitionId], *pair)
	}

	return nil
}

func (rmp *ReplicatedMapProxy) Get(key interface{}) (value interface{}, err error) {
	if err = AssertNotNil(key, "key"); err != nil {
		return nil, err
	}
	keyData, err := rmp.ToData(key)
	if err != nil {
		return nil, err
	}
	request := ReplicatedMapGetEncodeRequest(rmp.name, keyData)
	responseMessage, err := rmp.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapGetDecodeResponse(responseMessage).Response
	return rmp.ToObject(responseData)
}

func (rmp *ReplicatedMapProxy) ContainsKey(key interface{}) (found bool, err error) {
	if err = AssertNotNil(key, "key"); err != nil {
		return false, err
	}
	keyData, err := rmp.ToData(key)
	if err != nil {
		return false, err
	}
	request := ReplicatedMapContainsKeyEncodeRequest(rmp.name, keyData)
	responseMessage, err := rmp.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	response := ReplicatedMapContainsKeyDecodeResponse(responseMessage).Response
	return response, nil
}

func (rmp *ReplicatedMapProxy) ContainsValue(value interface{}) (found bool, err error) {
	if err = AssertNotNil(value, "value"); err != nil {
		return false, err
	}
	valueData, err := rmp.ToData(value)
	if err != nil {
		return false, err
	}
	request := ReplicatedMapContainsValueEncodeRequest(rmp.name, valueData)
	responseMessage, err := rmp.Invoke(request)
	if err != nil {
		return false, err
	}
	response := ReplicatedMapContainsValueDecodeResponse(responseMessage).Response
	return response, nil
}

func (rmp *ReplicatedMapProxy) Clear() (err error) {
	request := ReplicatedMapClearEncodeRequest(rmp.name)
	_, err = rmp.InvokeOnRandomTarget(request)
	return err
}

func (rmp *ReplicatedMapProxy) Remove(key interface{}) (value interface{}, err error) {
	if err = AssertNotNil(key, "key"); err != nil {
		return false, err
	}
	keyData, err := rmp.ToData(key)
	if err != nil {
		return false, err
	}
	request := ReplicatedMapRemoveEncodeRequest(rmp.name, keyData)
	responseMessage, err := rmp.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := ReplicatedMapRemoveDecodeResponse(responseMessage).Response
	return rmp.ToObject(responseData)
}

func (rmp *ReplicatedMapProxy) IsEmpty() (empty bool, err error) {
	request := ReplicatedMapIsEmptyEncodeRequest(rmp.name)
	responseMessage, err := rmp.Invoke(request)
	if err != nil {
		return false, err
	}
	response := MapIsEmptyDecodeResponse(responseMessage).Response
	return response, nil
}

func (rmp *ReplicatedMapProxy) Size() (size int32, err error) {
	request := ReplicatedMapSizeEncodeRequest(rmp.name)
	responseMessage, err := rmp.Invoke(request)
	if err != nil {
		return 0, err
	}
	response := ReplicatedMapSizeDecodeResponse(responseMessage).Response
	return response, nil
}

func (rmp *ReplicatedMapProxy) Values() (values []interface{}, err error) {
	request := ReplicatedMapValuesEncodeRequest(rmp.name)
	responseMessage, err := rmp.Invoke(request)
	if err != nil {
		return nil, err
	}
	response := ReplicatedMapValuesDecodeResponse(responseMessage).Response
	values = make([]interface{}, len(*response))
	for index, valueData := range *response {
		value, err := rmp.ToObject(&valueData)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}

func (rmp *ReplicatedMapProxy) KeySet() (keySet []interface{}, err error) {
	request := ReplicatedMapKeySetEncodeRequest(rmp.name)
	responseMessage, err := rmp.Invoke(request)
	if err != nil {
		return nil, err
	}
	response := ReplicatedMapKeySetDecodeResponse(responseMessage).Response
	keyList := make([]interface{}, len(*response))
	for index, keyData := range *response {
		key, err := rmp.ToObject(&keyData)
		if err != nil {
			return nil, err
		}
		keyList[index] = key
	}
	return keyList, nil
}

func (rmp *ReplicatedMapProxy) EntrySet() (resultPairs []IPair, err error) {
	request := ReplicatedMapEntrySetEncodeRequest(rmp.name)
	responseMessage, err := rmp.Invoke(request)
	if err != nil {
		return nil, err
	}

	response := ReplicatedMapEntrySetDecodeResponse(responseMessage).Response
	pairList := make([]IPair, len(*response))
	for index, pairData := range *response {
		key, err := rmp.ToObject(pairData.Key().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		value, err := rmp.ToObject(pairData.Value().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		pairList[index] = IPair(NewPair(key, value))
	}
	return pairList, nil
}

func (rmp *ReplicatedMapProxy) AddEntryListener(listener interface{}) (registrationID *string, err error) {
	return nil,nil
	/*var request *ClientMessage

	request = ReplicatedMapAddEntryListenerEncodeRequest(rmp.name, false)
	eventHandler := func(clientMessage *ClientMessage) {
		ReplicatedMapAddEntryListenerHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return rmp.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *ClientMessage {
		return ReplicatedMapRemoveEntryListenerEncodeRequest(rmp.name, registrationId)
	}, func(clientMessage *ClientMessage) *string {
		return ReplicatedMapAddEntryListenerDecodeResponse(clientMessage).Response
	})*/
}

func (rmp *ReplicatedMapProxy) onEntryEvent(keyData *serialization.Data, oldValueData *serialization.Data, valueData *serialization.Data, mergingValueData *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32, includedValue bool, listener interface{}) {
	key, _ := rmp.ToObject(keyData)
	oldValue, _ := rmp.ToObject(oldValueData)
	value, _ := rmp.ToObject(valueData)
	mergingValue, _ := rmp.ToObject(mergingValueData)
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

func (rmp *ReplicatedMapProxy) AddEntryListenerWithPredicate(listener interface{}, predicate IPredicate, includeValue bool) (registrationID *string, err error) {
	return nil, nil
}

func (rmp *ReplicatedMapProxy) AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID *string, err error) {
	return nil, nil
}

func (rmp *ReplicatedMapProxy) AddEntryListenerToKeyWithPredicate(listener interface{}, predicate IPredicate, key interface{}, includeValue bool) (registrationID *string, err error) {
	return nil, nil
}

func (rmp *ReplicatedMapProxy) RemoveEntryListener(registrationId *string) (removed bool, err error) {
	return false, nil
}
