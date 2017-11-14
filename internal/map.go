// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
	"errors"
	"github.com/hazelcast/go-client/core"
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"
	. "github.com/hazelcast/go-client/serialization"
	"time"
)

const (
	THREAD_ID = 1
	TTL       = 0
)

type MapProxy struct {
	*proxy
}

func (imap *MapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	if !CheckNotNil(key) {
		return nil, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	if !CheckNotNil(value) {
		return nil, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return nil, err
	}
	request := MapPutEncodeRequest(imap.name, keyData, valueData, THREAD_ID, TTL)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapPutDecodeResponse(responseMessage).Response
	return imap.ToObject(responseData)
}
func (imap *MapProxy) TryPut(key interface{}, value interface{}) (ok bool, err error) {
	if !CheckNotNil(key) {
		return false, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	if !CheckNotNil(value) {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}

	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return false, err
	}
	request := MapTryPutEncodeRequest(imap.name, keyData, valueData, THREAD_ID, TTL)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	ok = MapTryPutDecodeResponse(responseMessage).Response
	return ok, nil
}
func (imap *MapProxy) PutTransient(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) (err error) {
	if !CheckNotNil(key) {
		return errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	if !CheckNotNil(value) {
		return errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return err
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return err
	}
	ttl = GetTimeInMilliSeconds(ttl, ttlTimeUnit)
	request := MapPutTransientEncodeRequest(imap.name, keyData, valueData, THREAD_ID, ttl)
	_, err = imap.InvokeOnKey(request, keyData)
	return err
}
func (imap *MapProxy) Get(key interface{}) (value interface{}, err error) {
	if !CheckNotNil(key) {
		return nil, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	request := MapGetEncodeRequest(imap.name, keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapGetDecodeResponse(responseMessage).Response
	return imap.ToObject(responseData)
}
func (imap *MapProxy) Remove(key interface{}) (value interface{}, err error) {
	if !CheckNotNil(key) {
		return nil, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	request := MapRemoveEncodeRequest(imap.name, keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapRemoveDecodeResponse(responseMessage).Response
	return imap.ToObject(responseData)
}
func (imap *MapProxy) RemoveIfSame(key interface{}, value interface{}) (ok bool, err error) {
	if !CheckNotNil(key) {
		return false, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	if !CheckNotNil(value) {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return false, err
	}
	request := MapRemoveIfSameEncodeRequest(imap.name, keyData, valueData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	response := MapRemoveIfSameDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) RemoveAll(predicate IPredicate) (err error) {
	if predicate == nil {
		return NewHazelcastSerializationError("predicate should not be nil", nil)
	}
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return err
	}
	request := MapRemoveAllEncodeRequest(imap.name, predicateData)
	_, err = imap.InvokeOnRandomTarget(request)
	return err
}
func (imap *MapProxy) TryRemove(key interface{}, timeout int64, timeoutTimeUnit time.Duration) (ok bool, err error) {
	if !CheckNotNil(key) {
		return false, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	timeout = GetTimeInMilliSeconds(timeout, timeoutTimeUnit)
	request := MapTryRemoveEncodeRequest(imap.name, keyData, THREAD_ID, timeout)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	response := MapTryRemoveDecodeResponse(responseMessage).Response
	return response, nil

}
func (imap *MapProxy) Size() (size int32, err error) {
	request := MapSizeEncodeRequest(imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return -1, err
	}
	response := MapSizeDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) ContainsKey(key interface{}) (found bool, err error) {
	if !CheckNotNil(key) {
		return false, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	request := MapContainsKeyEncodeRequest(imap.name, keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	response := MapContainsKeyDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) ContainsValue(value interface{}) (found bool, err error) {
	if !CheckNotNil(value) {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return false, err
	}
	request := MapContainsValueEncodeRequest(imap.name, valueData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return false, err
	}
	response := MapContainsValueDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) Clear() (err error) {
	request := MapClearEncodeRequest(imap.name)
	_, err = imap.InvokeOnRandomTarget(request)
	if err != nil {
		return err
	}
	return nil
}
func (imap *MapProxy) Delete(key interface{}) (err error) {
	if !CheckNotNil(key) {
		return errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return err
	}
	request := MapDeleteEncodeRequest(imap.name, keyData, THREAD_ID)
	_, err = imap.InvokeOnKey(request, keyData)
	if err != nil {
		return err
	}
	return nil
}
func (imap *MapProxy) IsEmpty() (empty bool, err error) {
	request := MapIsEmptyEncodeRequest(imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return false, err
	}
	response := MapIsEmptyDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) AddIndex(attribute string, ordered bool) (err error) {
	request := MapAddIndexEncodeRequest(imap.name, &attribute, ordered)
	_, err = imap.InvokeOnRandomTarget(request)
	return err
}
func (imap *MapProxy) Evict(key interface{}) (evicted bool, err error) {
	if !CheckNotNil(key) {
		return false, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	request := MapEvictEncodeRequest(imap.name, keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	response := MapEvictDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) EvictAll() (err error) {
	request := MapEvictAllEncodeRequest(imap.name)
	_, err = imap.InvokeOnRandomTarget(request)
	if err != nil {
		return err
	}
	return nil
}
func (imap *MapProxy) Flush() (err error) {
	request := MapFlushEncodeRequest(imap.name)
	_, err = imap.InvokeOnRandomTarget(request)
	if err != nil {
		return err
	}
	return nil
}
func (imap *MapProxy) Lock(key interface{}) (err error) {
	return imap.LockWithLeaseTime(key, -1, time.Second)
}
func (imap *MapProxy) LockWithLeaseTime(key interface{}, lease int64, leaseTimeUnit time.Duration) (err error) {
	if !CheckNotNil(key) {
		return errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return err
	}
	//TODO :: What should be the reference id ?
	lease = GetTimeInMilliSeconds(lease, leaseTimeUnit)
	request := MapLockEncodeRequest(imap.name, keyData, THREAD_ID, lease, imap.client.ProxyManager.nextReferenceId())
	_, err = imap.InvokeOnKey(request, keyData)
	return err
}
func (imap *MapProxy) TryLock(key interface{}) (locked bool, err error) {
	return imap.TryLockWithTimeout(key, 0, time.Second)
}
func (imap *MapProxy) TryLockWithTimeout(key interface{}, timeout int64, timeoutTimeUnit time.Duration) (locked bool, err error) {
	return imap.TryLockWithTimeoutAndLease(key, timeout, timeoutTimeUnit, -1, time.Second)
}
func (imap *MapProxy) TryLockWithTimeoutAndLease(key interface{}, timeout int64, timeoutTimeUnit time.Duration, lease int64, leaseTimeUnit time.Duration) (locked bool, err error) {
	if !CheckNotNil(key) {
		return false, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	timeout = GetTimeInMilliSeconds(timeout, timeoutTimeUnit)
	lease = GetTimeInMilliSeconds(lease, leaseTimeUnit)
	request := MapTryLockEncodeRequest(imap.name, keyData, THREAD_ID, lease, timeout, imap.client.ProxyManager.nextReferenceId())
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	response := MapTryLockDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) Unlock(key interface{}) (err error) {
	if !CheckNotNil(key) {
		return errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return err
	}
	//TODO :: What should be the reference id ?
	request := MapUnlockEncodeRequest(imap.name, keyData, THREAD_ID, imap.client.ProxyManager.nextReferenceId())
	_, err = imap.InvokeOnKey(request, keyData)
	return err
}
func (imap *MapProxy) ForceUnlock(key interface{}) (err error) {
	if !CheckNotNil(key) {
		return errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return err
	}
	request := MapForceUnlockEncodeRequest(imap.name, keyData, imap.client.ProxyManager.nextReferenceId())
	_, err = imap.InvokeOnKey(request, keyData)
	return err
}
func (imap *MapProxy) IsLocked(key interface{}) (locked bool, err error) {
	if !CheckNotNil(key) {
		return false, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	//TODO :: What should be the reference id ?
	request := MapIsLockedEncodeRequest(imap.name, keyData)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	response := MapIsLockedDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) Replace(key interface{}, value interface{}) (oldValue interface{}, err error) {
	if !CheckNotNil(key) {
		return nil, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	if !CheckNotNil(value) {
		return nil, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return nil, err
	}
	request := MapReplaceEncodeRequest(imap.name, keyData, valueData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	responseData := MapReplaceDecodeResponse(responseMessage).Response
	return imap.ToObject(responseData)
}
func (imap *MapProxy) ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (replaced bool, err error) {
	if !CheckNotNil(key) {
		return false, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	if !CheckNotNil(newValue) {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	if !CheckNotNil(oldValue) {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	oldValueData, err := imap.ToData(oldValue)
	if err != nil {
		return false, err
	}
	newValueData, err := imap.ToData(newValue)
	if err != nil {
		return false, err
	}
	request := MapReplaceIfSameEncodeRequest(imap.name, keyData, oldValueData, newValueData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	response := MapRemoveIfSameDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) Set(key interface{}, value interface{}) (err error) {
	return imap.SetWithTtl(key, value, TTL, time.Second)
}
func (imap *MapProxy) SetWithTtl(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) (err error) {
	if !CheckNotNil(key) {
		return errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	if !CheckNotNil(value) {
		return errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return err
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return err
	}
	ttl = GetTimeInMilliSeconds(ttl, ttlTimeUnit)
	request := MapSetEncodeRequest(imap.name, keyData, valueData, THREAD_ID, ttl)
	_, err = imap.InvokeOnKey(request, keyData)
	if err != nil {
		return err
	}
	return nil
}

func (imap *MapProxy) PutIfAbsent(key interface{}, value interface{}) (oldValue interface{}, err error) {
	if !CheckNotNil(key) {
		return nil, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	if !CheckNotNil(value) {
		return nil, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return nil, err
	}
	request := MapPutIfAbsentEncodeRequest(imap.name, keyData, valueData, THREAD_ID, TTL)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapPutIfAbsentDecodeResponse(responseMessage).Response
	return imap.ToObject(responseData)
}
func (imap *MapProxy) PutAll(mp map[interface{}]interface{}) (err error) {
	if !CheckNotNil(mp) {
		return errors.New("Null argument map is not allowed")
	}
	partitions := make(map[int32][]Pair)
	for key, value := range mp {
		keyData, err := imap.ToData(key)
		if err != nil {
			return err
		}
		valueData, err := imap.ToData(value)
		if err != nil {
			return err
		}
		pair := NewPair(keyData, valueData)
		partitionId := imap.client.PartitionService.GetPartitionId(keyData)
		partitions[partitionId] = append(partitions[partitionId], *pair)
	}
	for partitionId, entryList := range partitions {
		request := MapPutAllEncodeRequest(imap.name, &entryList)
		_, err := imap.InvokeOnPartition(request, partitionId)
		if err != nil {
			return err
		}
	}
	return nil
}
func (imap *MapProxy) KeySet() (keySet []interface{}, err error) {
	request := MapKeySetEncodeRequest(imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}
	response := MapKeySetDecodeResponse(responseMessage).Response
	keyList := make([]interface{}, len(*response))
	for index, keyData := range *response {
		key, err := imap.ToObject(&keyData)
		if err != nil {
			return nil, err
		}
		keyList[index] = key
	}
	return keyList, nil
}
func (imap *MapProxy) KeySetWithPredicate(predicate IPredicate) (keySet []interface{}, err error) {
	if predicate == nil {
		return nil, NewHazelcastSerializationError("predicate should not be nil", nil)
	}
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return nil, err
	}
	request := MapKeySetWithPredicateEncodeRequest(imap.name, predicateData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}
	response := MapKeySetWithPredicateDecodeResponse(responseMessage).Response
	keyList := make([]interface{}, len(*response))
	for index, keyData := range *response {
		key, err := imap.ToObject(&keyData)
		if err != nil {
			return nil, err
		}
		keyList[index] = key
	}
	return keyList, nil
}
func (imap *MapProxy) Values() (values []interface{}, err error) {
	request := MapValuesEncodeRequest(imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}
	response := MapValuesDecodeResponse(responseMessage).Response
	valueList := make([]interface{}, len(*response))
	for index, valueData := range *response {
		value, err := imap.ToObject(&valueData)
		if err != nil {
			return nil, err
		}
		valueList[index] = value
	}
	return valueList, nil
}
func (imap *MapProxy) ValuesWithPredicate(predicate IPredicate) (values []interface{}, err error) {
	if predicate == nil {
		return nil, NewHazelcastSerializationError("predicate should not be nil", nil)
	}
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return nil, err
	}
	request := MapValuesWithPredicateEncodeRequest(imap.name, predicateData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}
	response := MapValuesWithPredicateDecodeResponse(responseMessage).Response
	valueList := make([]interface{}, len(*response))
	for index, valueData := range *response {
		value, err := imap.ToObject(&valueData)
		if err != nil {
			return nil, err
		}
		valueList[index] = value
	}
	return valueList, nil
}
func (imap *MapProxy) EntrySet() (resultPairs []core.IPair, err error) {
	request := MapEntrySetEncodeRequest(imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}

	response := MapEntrySetDecodeResponse(responseMessage).Response
	pairList := make([]core.IPair, len(*response))
	for index, pairData := range *response {
		key, err := imap.ToObject(pairData.Key().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		value, err := imap.ToObject(pairData.Value().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		pairList[index] = core.IPair(NewPair(key, value))
	}
	return pairList, nil
}
func (imap *MapProxy) EntrySetWithPredicate(predicate IPredicate) (resultPairs []core.IPair, err error) {
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return nil, err
	}
	request := MapEntriesWithPredicateEncodeRequest(imap.name, predicateData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}
	response := MapEntriesWithPredicateDecodeResponse(responseMessage).Response
	pairList := make([]core.IPair, len(*response))
	for index, pairData := range *response {
		key, err := imap.ToObject(pairData.Key().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		value, err := imap.ToObject(pairData.Value().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		pairList[index] = core.IPair(NewPair(key, value))
	}
	return pairList, nil
}
func (imap *MapProxy) GetAll(keys []interface{}) (entryMap map[interface{}]interface{}, err error) {
	if !CheckNotEmpty(keys) {
		return nil, errors.New(NIL_KEYS_ARE_NOT_ALLOWED)
	}
	partitions := make(map[int32][]serialization.Data)
	entryMap = make(map[interface{}]interface{}, 0)
	for _, key := range keys {
		keyData, err := imap.ToData(key)
		if err != nil {
			return nil, err
		}
		partitionId := imap.client.PartitionService.GetPartitionId(keyData)
		partitions[partitionId] = append(partitions[partitionId], *keyData)
	}
	for partitionId, keyList := range partitions {
		request := MapGetAllEncodeRequest(imap.name, &keyList)
		responseMessage, err := imap.InvokeOnPartition(request, partitionId)
		if err != nil {
			return nil, err
		}
		response := MapGetAllDecodeResponse(responseMessage).Response
		for _, pairData := range *response {
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
func (imap *MapProxy) GetEntryView(key interface{}) (entryView core.IEntryView, err error) {
	if !CheckNotNil(key) {
		return nil, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	request := MapGetEntryViewEncodeRequest(imap.name, keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	response := MapGetEntryViewDecodeResponse(responseMessage).Response
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
		return MapAddEntryListenerDecodeResponse(clientMessage).Response
	})
}
func (imap *MapProxy) AddEntryListenerWithPredicate(listener interface{}, predicate IPredicate, includeValue bool) (*string, error) {
	var request *ClientMessage
	listenerFlags := GetEntryListenerFlags(listener)
	predicateData, err := imap.ToData(predicate)
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
			return MapAddEntryListenerWithPredicateDecodeResponse(clientMessage).Response
		})
}
func (imap *MapProxy) AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID *string, err error) {
	var request *ClientMessage
	listenerFlags := GetEntryListenerFlags(listener)
	keyData, err := imap.ToData(key)
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
		return MapAddEntryListenerToKeyDecodeResponse(clientMessage).Response
	})
}
func (imap *MapProxy) AddEntryListenerToKeyWithPredicate(listener interface{}, predicate IPredicate, key interface{}, includeValue bool) (*string, error) {
	var request *ClientMessage
	listenerFlags := GetEntryListenerFlags(listener)
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	predicateData, err := imap.ToData(predicate)
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
			return MapAddEntryListenerToKeyWithPredicateDecodeResponse(clientMessage).Response
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
	return imap.client.ListenerService.deregisterListenerInternal(*registrationId, func(registrationId *string) *ClientMessage {
		return MapRemoveEntryListenerEncodeRequest(imap.name, registrationId)
	})
}

func (imap *MapProxy) ExecuteOnKey(key interface{}, entryProcessor interface{}) (result interface{}, err error) {
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	entryProcessorData, err := imap.ToData(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := MapExecuteOnKeyEncodeRequest(imap.name, entryProcessorData, keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapExecuteOnKeyDecodeResponse(responseMessage).Response
	return imap.ToObject(responseData)
}
func (imap *MapProxy) ExecuteOnKeys(keys []interface{}, entryProcessor interface{}) (keyToResultPairs []core.IPair, err error) {
	keysData := make([]serialization.Data, len(keys))
	for index, key := range keys {
		keyData, err := imap.ToData(key)
		if err != nil {
			return nil, err
		}
		keysData[index] = *keyData
	}
	entryProcessorData, err := imap.ToData(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := MapExecuteOnKeysEncodeRequest(imap.name, entryProcessorData, &keysData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}
	responseData := MapExecuteOnKeysDecodeResponse(responseMessage).Response
	pairList := make([]core.IPair, len(*responseData))
	for index, pairData := range *responseData {
		key, err := imap.ToObject(pairData.Key().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		value, err := imap.ToObject(pairData.Value().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		pairList[index] = core.IPair(NewPair(key, value))
	}
	return pairList, nil
}
func (imap *MapProxy) ExecuteOnEntries(entryProcessor interface{}) (keyToResultPairs []core.IPair, err error) {
	entryProcessorData, err := imap.ToData(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := MapExecuteOnAllKeysEncodeRequest(imap.name, entryProcessorData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}
	responseData := MapExecuteOnAllKeysDecodeResponse(responseMessage).Response
	pairList := make([]core.IPair, len(*responseData))
	for index, pairData := range *responseData {
		key, err := imap.ToObject(pairData.Key().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		value, err := imap.ToObject(pairData.Value().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		pairList[index] = core.IPair(NewPair(key, value))
	}
	return pairList, nil
}

func (imap *MapProxy) ExecuteOnEntriesWithPredicate(entryProcessor interface{}, predicate IPredicate) ([]core.IPair, error) {
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return nil, err
	}
	entryProcessorData, err := imap.ToData(entryProcessor)
	if err != nil {
		return nil, err
	}
	request := MapExecuteWithPredicateEncodeRequest(imap.name, entryProcessorData, predicateData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}
	responseData := MapExecuteWithPredicateDecodeResponse(responseMessage).Response
	pairList := make([]core.IPair, len(*responseData))
	for index, pairData := range *responseData {
		key, err := imap.ToObject(pairData.Key().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		value, err := imap.ToObject(pairData.Value().(*serialization.Data))
		if err != nil {
			return nil, err
		}
		pairList[index] = core.IPair(NewPair(key, value))
	}
	return pairList, nil
}
