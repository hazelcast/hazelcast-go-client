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

type multiMapProxy struct {
	*proxy
}

func newMultiMapProxy(client *HazelcastClient, serviceName *string, name *string) (*multiMapProxy, error) {
	return &multiMapProxy{&proxy{client, serviceName, name}}, nil
}

func (mmp *multiMapProxy) Put(key interface{}, value interface{}) (increased bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := protocol.MultiMapPutEncodeRequest(mmp.name, keyData, valueData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapPutDecodeResponse)
}

func (mmp *multiMapProxy) Get(key interface{}) (values []interface{}, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := protocol.MultiMapGetEncodeRequest(mmp.name, keyData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MultiMapGetDecodeResponse)
}

func (mmp *multiMapProxy) Remove(key interface{}, value interface{}) (removed bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := protocol.MultiMapRemoveEntryEncodeRequest(mmp.name, keyData, valueData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapRemoveEntryDecodeResponse)

}

func (mmp *multiMapProxy) RemoveAll(key interface{}) (oldValues []interface{}, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := protocol.MultiMapRemoveEncodeRequest(mmp.name, keyData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MultiMapRemoveDecodeResponse)
}

func (mmp *multiMapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := protocol.MultiMapContainsKeyEncodeRequest(mmp.name, keyData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapContainsKeyDecodeResponse)
}

func (mmp *multiMapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := mmp.validateAndSerialize(value)
	if err != nil {
		return
	}
	request := protocol.MultiMapContainsValueEncodeRequest(mmp.name, valueData)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapContainsValueDecodeResponse)
}

func (mmp *multiMapProxy) ContainsEntry(key interface{}, value interface{}) (found bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := protocol.MultiMapContainsEntryEncodeRequest(mmp.name, keyData, valueData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapContainsEntryDecodeResponse)
}

func (mmp *multiMapProxy) Clear() (err error) {
	request := protocol.MultiMapClearEncodeRequest(mmp.name)
	_, err = mmp.invokeOnRandomTarget(request)
	return
}

func (mmp *multiMapProxy) Size() (size int32, err error) {
	request := protocol.MultiMapSizeEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInt32AndError(responseMessage, err, protocol.MultiMapSizeDecodeResponse)
}

func (mmp *multiMapProxy) ValueCount(key interface{}) (valueCount int32, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := protocol.MultiMapValueCountEncodeRequest(mmp.name, keyData, threadID)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToInt32AndError(responseMessage, err, protocol.MultiMapValueCountDecodeResponse)
}

func (mmp *multiMapProxy) Values() (values []interface{}, err error) {
	request := protocol.MultiMapValuesEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MultiMapValuesDecodeResponse)
}

func (mmp *multiMapProxy) KeySet() (keySet []interface{}, err error) {
	request := protocol.MultiMapKeySetEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToInterfaceSliceAndError(responseMessage, err, protocol.MultiMapKeySetDecodeResponse)
}

func (mmp *multiMapProxy) EntrySet() (resultPairs []core.Pair, err error) {
	request := protocol.MultiMapEntrySetEncodeRequest(mmp.name)
	responseMessage, err := mmp.invokeOnRandomTarget(request)
	return mmp.decodeToPairSliceAndError(responseMessage, err, protocol.MultiMapEntrySetDecodeResponse)
}

func (mmp *multiMapProxy) AddEntryListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	request := protocol.MultiMapAddEntryListenerEncodeRequest(mmp.name, includeValue, mmp.isSmart())
	eventHandler := mmp.createEventHandler(listener)
	return mmp.client.ListenerService.registerListener(request, eventHandler, func(registrationID *string) *protocol.ClientMessage {
		return protocol.MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationID)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.MultiMapAddEntryListenerDecodeResponse(clientMessage)()
	})
}

func (mmp *multiMapProxy) AddEntryListenerToKey(listener interface{}, key interface{},
	includeValue bool) (registrationID *string, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := protocol.MultiMapAddEntryListenerToKeyEncodeRequest(mmp.name, keyData, includeValue, mmp.isSmart())
	eventHandler := mmp.createEventHandlerToKey(listener)
	return mmp.client.ListenerService.registerListener(request, eventHandler, func(registrationID *string) *protocol.ClientMessage {
		return protocol.MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationID)
	}, func(clientMessage *protocol.ClientMessage) *string {
		return protocol.MultiMapAddEntryListenerToKeyDecodeResponse(clientMessage)()
	})
}

func (mmp *multiMapProxy) RemoveEntryListener(registrationID *string) (removed bool, err error) {
	return mmp.client.ListenerService.deregisterListener(*registrationID, func(registrationID *string) *protocol.ClientMessage {
		return protocol.MultiMapRemoveEntryListenerEncodeRequest(mmp.name, registrationID)
	})
}

func (mmp *multiMapProxy) Lock(key interface{}) (err error) {
	return mmp.LockWithLeaseTime(key, -1)
}

func (mmp *multiMapProxy) LockWithLeaseTime(key interface{}, lease time.Duration) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	leaseInMillis := timeutil.GetTimeInMilliSeconds(lease)
	request := protocol.MultiMapLockEncodeRequest(mmp.name, keyData, threadID, leaseInMillis,
		mmp.client.ProxyManager.nextReferenceID())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *multiMapProxy) IsLocked(key interface{}) (locked bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	request := protocol.MultiMapIsLockedEncodeRequest(mmp.name, keyData)
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapIsLockedDecodeResponse)
}

func (mmp *multiMapProxy) TryLock(key interface{}) (locked bool, err error) {
	return mmp.TryLockWithTimeout(key, 0)
}

func (mmp *multiMapProxy) TryLockWithTimeout(key interface{}, timeout time.Duration) (locked bool, err error) {
	return mmp.TryLockWithTimeoutAndLease(key, timeout, -1)
}

func (mmp *multiMapProxy) TryLockWithTimeoutAndLease(key interface{}, timeout time.Duration,
	lease time.Duration) (locked bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return false, err
	}
	timeoutInMillis := timeutil.GetTimeInMilliSeconds(timeout)
	leaseInMillis := timeutil.GetTimeInMilliSeconds(lease)
	request := protocol.MultiMapTryLockEncodeRequest(mmp.name, keyData, threadID, leaseInMillis, timeoutInMillis,
		mmp.client.ProxyManager.nextReferenceID())
	responseMessage, err := mmp.invokeOnKey(request, keyData)
	return mmp.decodeToBoolAndError(responseMessage, err, protocol.MultiMapTryLockDecodeResponse)
}

func (mmp *multiMapProxy) Unlock(key interface{}) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MultiMapUnlockEncodeRequest(mmp.name, keyData, threadID, mmp.client.ProxyManager.nextReferenceID())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *multiMapProxy) ForceUnlock(key interface{}) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return err
	}
	request := protocol.MultiMapForceUnlockEncodeRequest(mmp.name, keyData, mmp.client.ProxyManager.nextReferenceID())
	_, err = mmp.invokeOnKey(request, keyData)
	return
}

func (mmp *multiMapProxy) onEntryEvent(keyData *serialization.Data, oldValueData *serialization.Data,
	valueData *serialization.Data, mergingValueData *serialization.Data, eventType int32, uuid *string,
	numberOfAffectedEntries int32, listener interface{}) {
	key, _ := mmp.toObject(keyData)
	oldValue, _ := mmp.toObject(oldValueData)
	value, _ := mmp.toObject(valueData)
	mergingValue, _ := mmp.toObject(mergingValueData)
	entryEvent := protocol.NewEntryEvent(key, oldValue, value, mergingValue, eventType, uuid)
	mapEvent := protocol.NewMapEvent(eventType, uuid, numberOfAffectedEntries)
	switch eventType {
	case bufutil.EntryEventAdded:
		listener.(protocol.EntryAddedListener).EntryAdded(entryEvent)
	case bufutil.EntryEventRemoved:
		listener.(protocol.EntryRemovedListener).EntryRemoved(entryEvent)
	case bufutil.EntryEventClearAll:
		listener.(protocol.EntryClearAllListener).EntryClearAll(mapEvent)
	}
}

func (mmp *multiMapProxy) createEventHandler(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.MultiMapAddEntryListenerHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, uuid *string, numberOfAffectedEntries int32) {
			mmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, listener)
		})
	}
}

func (mmp *multiMapProxy) createEventHandlerToKey(listener interface{}) func(clientMessage *protocol.ClientMessage) {
	return func(clientMessage *protocol.ClientMessage) {
		protocol.MultiMapAddEntryListenerToKeyHandle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data,
			value *serialization.Data, mergingValue *serialization.Data, eventType int32, uuid *string, numberOfAffectedEntries int32) {
			mmp.onEntryEvent(key, oldValue, value, mergingValue, eventType, uuid, numberOfAffectedEntries, listener)
		})
	}
}
