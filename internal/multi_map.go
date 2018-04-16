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
	request := protocol.MultiMapPutEncodeRequest(mmp.name, keyData, valueData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapPutDecodeResponse)
}

func (mmp *MultiMapProxy) Get(key interface{}) (values []interface{}, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := protocol.MultiMapGetEncodeRequest(mmp.name, keyData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MultiMapGetDecodeResponse)
}

func (mmp *MultiMapProxy) Remove(key interface{}, value interface{}) (removed bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := protocol.MultiMapRemoveEntryEncodeRequest(mmp.name, keyData, valueData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapRemoveEntryDecodeResponse)

}

func (mmp *MultiMapProxy) RemoveAll(key interface{}) (oldValues []interface{}, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := protocol.MultiMapRemoveEncodeRequest(mmp.name, keyData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MultiMapRemoveDecodeResponse)
}

func (mmp *MultiMapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := protocol.MultiMapContainsKeyEncodeRequest(mmp.name, keyData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapContainsKeyDecodeResponse)
}

func (mmp *MultiMapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := mmp.validateAndSerialize(value)
	if err != nil {
		return
	}
	request := protocol.MultiMapContainsValueEncodeRequest(mmp.name, valueData)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapContainsValueDecodeResponse)
}

func (mmp *MultiMapProxy) ContainsEntry(key interface{}, value interface{}) (found bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := protocol.MultiMapContainsEntryEncodeRequest(mmp.name, keyData, valueData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapContainsEntryDecodeResponse)
}

func (mmp *MultiMapProxy) Clear() (err error) {
	request := protocol.MultiMapClearEncodeRequest(mmp.name)
	_, err = mmp.invokeOnRandomTarget(request)
	return
}

func (mmp *MultiMapProxy) Size() (size int32, err error) {
	request := protocol.MultiMapSizeEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInt32AndError(responseMessage, err, protocol.MultiMapSizeDecodeResponse)
}

func (mmp *MultiMapProxy) ValueCount(key interface{}) (valueCount int32, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := protocol.MultiMapValueCountEncodeRequest(mmp.name, keyData, threadId)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInt32AndError(responseMessage, err, protocol.MultiMapValueCountDecodeResponse)
}

func (mmp *MultiMapProxy) Values() (values []interface{}, err error) {
	request := protocol.MultiMapValuesEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MultiMapValuesDecodeResponse)
}

func (mmp *MultiMapProxy) KeySet() (keySet []interface{}, err error) {
	request := protocol.MultiMapKeySetEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MultiMapKeySetDecodeResponse)
}

func (mmp *MultiMapProxy) EntrySet() (resultPairs []core.IPair, err error) {
	request := protocol.MultiMapEntrySetEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToPairSliceAndError(responseMessage, err, protocol.MultiMapEntrySetDecodeResponse)
}

func (mmp *MultiMapProxy) AddEntryListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	var request *protocol.ClientMessage
	request = protocol.MultiMapAddEntryListenerEncodeRequest(mmp.name, includeValue, mmp.isSmart())
	eventHandler := mmp.createEventHandler(listener)
	return mmp.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *protocol.ClientMessage {
		return protocol.MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationId)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.MultiMapAddEntryListenerDecodeResponse(clientMessage)()
	})
}

func (mmp *MultiMapProxy) AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID *string, err error) {
	var request *protocol.ClientMessage
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request = protocol.MultiMapAddEntryListenerToKeyEncodeRequest(mmp.name, keyData, includeValue, mmp.isSmart())
	eventHandler := mmp.createEventHandlerToKey(listener)
	return mmp.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *protocol.ClientMessage {
		return protocol.MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationId)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.MultiMapAddEntryListenerToKeyDecodeResponse(clientMessage)()
	})
}

func (mmp *MultiMapProxy) RemoveEntryListener(registrationId *string) (removed bool, err error) {
	return mmp.client.ListenerService.deregisterListener(*registrationId, func(registrationId *string) *protocol.ClientMessage {
		return protocol.MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationId)
	})
}

func (mmp *MultiMapProxy) Lock(key interface{}) (err error) {
	return mmp.LockWithLeaseTime(key, -1)
}

func (mmp *MultiMapProxy) LockWithLeaseTime(key interface{}, lease time.Duration) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	leaseInMillis := common.GetTimeInMilliSeconds(lease)
	request := protocol.MultiMapLockEncodeRequest(mmp.name, keyData, threadId, leaseInMillis, mmp.client.ProxyManager.nextReferenceId())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *MultiMapProxy) IsLocked(key interface{}) (locked bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.MultiMapIsLockedEncodeRequest(mmp.name, keyData)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapIsLockedDecodeResponse)
}

func (mmp *MultiMapProxy) TryLock(key interface{}) (locked bool, err error) {
	return mmp.TryLockWithTimeout(key, 0)
}

func (mmp *MultiMapProxy) TryLockWithTimeout(key interface{}, timeout time.Duration) (locked bool, err error) {
	return mmp.TryLockWithTimeoutAndLease(key, timeout, -1)
}

func (mmp *MultiMapProxy) TryLockWithTimeoutAndLease(key interface{}, timeout time.Duration, lease time.Duration) (locked bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	timeoutInMillis := common.GetTimeInMilliSeconds(timeout)
	leaseInMillis := common.GetTimeInMilliSeconds(lease)
	request := protocol.MultiMapTryLockEncodeRequest(mmp.name, keyData, threadId, leaseInMillis, timeoutInMillis, mmp.client.ProxyManager.nextReferenceId())
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapTryLockDecodeResponse)
}

func (mmp *MultiMapProxy) Unlock(key interface{}) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MultiMapUnlockEncodeRequest(mmp.name, keyData, threadId, mmp.client.ProxyManager.nextReferenceId())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *MultiMapProxy) ForceUnlock(key interface{}) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MultiMapForceUnlockEncodeRequest(mmp.name, keyData, mmp.client.ProxyManager.nextReferenceId())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *MultiMapProxy) onEntryEvent(keyData *serialization.Data, oldValueData *serialization.Data, valueData *serialization.Data, mergingValueData *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32, listener interface{}) {
	key, _ := mmp.toObject(keyData)
	oldValue, _ := mmp.toObject(oldValueData)
	value, _ := mmp.toObject(valueData)
	mergingValue, _ := mmp.toObject(mergingValueData)
	entryEvent := protocol.NewEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid)
	mapEvent := protocol.NewMapEvent(eventType, Uuid, numberOfAffectedEntries)
	switch eventType {
	case common.EntryEventAdded:
		listener.(protocol.EntryAddedListener).EntryAdded(entryEvent)
	case common.EntryEventRemoved:
		listener.(protocol.EntryRemovedListener).EntryRemoved(entryEvent)
	case common.EntryEventClearAll:
		listener.(protocol.EntryClearAllListener).EntryClearAll(mapEvent)
	}
}

func (mmp *MultiMapProxy) createEventHandler(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.MultiMapAddEntryListenerHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			mmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, listener)
		})
	}
}

func (mmp *MultiMapProxy) createEventHandlerToKey(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.MultiMapAddEntryListenerToKeyHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			mmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, listener)
		})
	}
}
