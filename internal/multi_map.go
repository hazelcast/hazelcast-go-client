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
	"time"
)

type MultiMapProxy struct {
	*proxy
}

func newMultiMapProxy(client *HazelcastClient, serviceName *string, name *string) (*MultiMapProxy, error) {
	return &MultiMapProxy{&proxy{client, serviceName, name}}, nil
}

func (mmp *MultiMapProxy) Put(key interface{}, value interface{}) (increased bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := MultiMapPutEncodeRequest(mmp.name, keyData, valueData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, MultiMapPutDecodeResponse)
}

func (mmp *MultiMapProxy) Get(key interface{}) (values []interface{}, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := MultiMapGetEncodeRequest(mmp.name, keyData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, MultiMapGetDecodeResponse)
}

func (mmp *MultiMapProxy) Remove(key interface{}, value interface{}) (removed bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := MultiMapRemoveEntryEncodeRequest(mmp.name, keyData, valueData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, MultiMapRemoveEntryDecodeResponse)

}

func (mmp *MultiMapProxy) RemoveAll(key interface{}) (oldValues []interface{}, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := MultiMapRemoveEncodeRequest(mmp.name, keyData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, MultiMapRemoveDecodeResponse)
}

func (mmp *MultiMapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := MultiMapContainsKeyEncodeRequest(mmp.name, keyData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, MultiMapContainsKeyDecodeResponse)
}

func (mmp *MultiMapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := mmp.validateAndSerialize(value)
	if err != nil {
		return
	}
	request := MultiMapContainsValueEncodeRequest(mmp.name, valueData)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToBoolAndError(responseMessage, err, MultiMapContainsValueDecodeResponse)
}

func (mmp *MultiMapProxy) ContainsEntry(key interface{}, value interface{}) (found bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := MultiMapContainsEntryEncodeRequest(mmp.name, keyData, valueData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, MultiMapContainsEntryDecodeResponse)
}

func (mmp *MultiMapProxy) Clear() (err error) {
	request := MultiMapClearEncodeRequest(mmp.name)
	_, err = mmp.invokeOnRandomTarget(request)
	return
}

func (mmp *MultiMapProxy) Size() (size int32, err error) {
	request := MultiMapSizeEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInt32AndError(responseMessage, err, MultiMapSizeDecodeResponse)
}

func (mmp *MultiMapProxy) ValueCount(key interface{}) (valueCount int32, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := MultiMapValueCountEncodeRequest(mmp.name, keyData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInt32AndError(responseMessage, err, MultiMapValueCountDecodeResponse)
}

func (mmp *MultiMapProxy) Values() (values []interface{}, err error) {
	request := MultiMapValuesEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, MultiMapValuesDecodeResponse)
}

func (mmp *MultiMapProxy) KeySet() (keySet []interface{}, err error) {
	request := MultiMapKeySetEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, MultiMapKeySetDecodeResponse)
}

func (mmp *MultiMapProxy) EntrySet() (resultPairs []IPair, err error) {
	request := MultiMapEntrySetEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToPairSliceAndError(responseMessage, err, MultiMapEntrySetDecodeResponse)
}

func (mmp *MultiMapProxy) AddEntryListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	var request *ClientMessage
	request = MultiMapAddEntryListenerEncodeRequest(mmp.name, includeValue, mmp.isSmart())
	eventHandler := mmp.createEventHandler(listener)
	return mmp.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *ClientMessage {
		return MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationId)
	}, func(clientMessage *ClientMessage) *string {
		return MultiMapAddEntryListenerDecodeResponse(clientMessage)()
	})
}

func (mmp *MultiMapProxy) AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID *string, err error) {
	var request *ClientMessage
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request = MultiMapAddEntryListenerToKeyEncodeRequest(mmp.name, keyData, includeValue, mmp.isSmart())
	eventHandler := mmp.createEventHandlerToKey(listener)
	return mmp.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *ClientMessage {
		return MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationId)
	}, func(clientMessage *ClientMessage) *string {
		return MultiMapAddEntryListenerToKeyDecodeResponse(clientMessage)()
	})
}

func (mmp *MultiMapProxy) RemoveEntryListener(registrationId *string) (removed bool, err error) {
	return mmp.client.ListenerService.deregisterListener(*registrationId, func(registrationId *string) *ClientMessage {
		return MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationId)
	})
}

func (mmp *MultiMapProxy) Lock(key interface{}) (err error) {
	return mmp.LockWithLeaseTime(key, -1, time.Second)
}

func (mmp *MultiMapProxy) LockWithLeaseTime(key interface{}, lease int64, leaseTimeUnit time.Duration) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	lease = GetTimeInMilliSeconds(lease, leaseTimeUnit)
	request := MultiMapLockEncodeRequest(mmp.name, keyData, threadId, lease, mmp.client.ProxyManager.nextReferenceId())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *MultiMapProxy) IsLocked(key interface{}) (locked bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := MultiMapIsLockedEncodeRequest(mmp.name, keyData)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, MultiMapIsLockedDecodeResponse)
}

func (mmp *MultiMapProxy) TryLock(key interface{}) (locked bool, err error) {
	return mmp.TryLockWithTimeout(key, 0, time.Second)
}

func (mmp *MultiMapProxy) TryLockWithTimeout(key interface{}, timeout int64, timeoutTimeUnit time.Duration) (locked bool, err error) {
	return mmp.TryLockWithTimeoutAndLease(key, timeout, timeoutTimeUnit, -1, time.Second)
}

func (mmp *MultiMapProxy) TryLockWithTimeoutAndLease(key interface{}, timeout int64, timeoutTimeUnit time.Duration, lease int64, leaseTimeUnit time.Duration) (locked bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	timeout = GetTimeInMilliSeconds(timeout, timeoutTimeUnit)
	lease = GetTimeInMilliSeconds(lease, leaseTimeUnit)
	request := MultiMapTryLockEncodeRequest(mmp.name, keyData, threadId, lease, timeout, mmp.client.ProxyManager.nextReferenceId())
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, MultiMapTryLockDecodeResponse)
}

func (mmp *MultiMapProxy) Unlock(key interface{}) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := MultiMapUnlockEncodeRequest(mmp.name, keyData, threadId, mmp.client.ProxyManager.nextReferenceId())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *MultiMapProxy) ForceUnlock(key interface{}) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := MultiMapForceUnlockEncodeRequest(mmp.name, keyData, mmp.client.ProxyManager.nextReferenceId())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *MultiMapProxy) onEntryEvent(keyData *serialization.Data, oldValueData *serialization.Data, valueData *serialization.Data, mergingValueData *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32, listener interface{}) {
	key, _ := mmp.toObject(keyData)
	oldValue, _ := mmp.toObject(oldValueData)
	value, _ := mmp.toObject(valueData)
	mergingValue, _ := mmp.toObject(mergingValueData)
	entryEvent := NewEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid)
	mapEvent := NewMapEvent(eventType, Uuid, numberOfAffectedEntries)
	switch eventType {
	case EntryEventAdded:
		listener.(EntryAddedListener).EntryAdded(entryEvent)
	case EntryEventRemoved:
		listener.(EntryRemovedListener).EntryRemoved(entryEvent)
	case EntryEventClearAll:
		listener.(EntryClearAllListener).EntryClearAll(mapEvent)
	}
}

func (mmp *MultiMapProxy) createEventHandler(listener interface{}) func(clientMessage *ClientMessage) {
	return func(clientMessage *ClientMessage) {
		MultiMapAddEntryListenerHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			mmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, listener)
		})
	}
}

func (mmp *MultiMapProxy) createEventHandlerToKey(listener interface{}) func(clientMessage *ClientMessage) {
	return func(clientMessage *ClientMessage) {
		MultiMapAddEntryListenerToKeyHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			mmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, listener)
		})
	}
}
