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
	"errors"
	. "github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	//"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	"time"
)

const (
	THREAD_ID int64 = 1
	TTL       int64 = 0
)

type MapProxy struct {
	*proxy
}

func newMapProxy(client *HazelcastClient, serviceName *string, name *string) *MapProxy {
	return &MapProxy{&proxy{client, serviceName, name}}
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
	return imap.EncodeInvokeOnKey(MapPutCodec, keyData, imap.name, keyData, valueData, THREAD_ID, TTL)
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
	res, err := imap.EncodeInvokeOnKey(MapTryPutCodec, keyData, imap.name, keyData, valueData, THREAD_ID, TTL)
	if err != nil {
		return false, err
	}
	return res.(bool), err

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
	_, err = imap.EncodeInvokeOnKey(MapPutTransientCodec, keyData, imap.name, keyData, valueData, THREAD_ID, ttl)
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
	return imap.EncodeInvokeOnKey(MapGetCodec, keyData, imap.name, keyData, THREAD_ID)
}
func (imap *MapProxy) Remove(key interface{}) (value interface{}, err error) {
	if !CheckNotNil(key) {
		return nil, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	return imap.EncodeInvokeOnKey(MapRemoveCodec, keyData, imap.name, keyData, THREAD_ID)

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
	res, err := imap.EncodeInvokeOnKey(MapRemoveIfSameCodec, keyData, imap.name, keyData, valueData, THREAD_ID)
	if err != nil {
		return false, err
	}
	return res.(bool), err

}
func (imap *MapProxy) RemoveAll(predicate IPredicate) (err error) {
	if predicate == nil {
		return NewHazelcastSerializationError("predicate should not be nil", nil)
	}
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return err
	}
	_, err = imap.EncodeInvoke(MapRemoveAllCodec, imap.name, predicateData)
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
	res, err := imap.EncodeInvokeOnKey(MapTryRemoveCodec, keyData, imap.name, keyData, THREAD_ID, timeout)
	if err != nil {
		return false, err
	}
	return res.(bool), err

}
func (imap *MapProxy) Size() (size int32, err error) {
	res, err := imap.EncodeInvoke(MapSizeCodec, imap.name)
	if err != nil {
		return 0, err
	}
	return res.(int32), err
}

func (imap *MapProxy) ContainsKey(key interface{}) (found bool, err error) {
	if !CheckNotNil(key) {
		return false, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	res, err := imap.EncodeInvokeOnKey(MapContainsKeyCodec, keyData, imap.name, keyData, THREAD_ID)
	if err != nil {
		return false, err
	}
	return res.(bool), err
}
func (imap *MapProxy) ContainsValue(value interface{}) (found bool, err error) {
	if !CheckNotNil(value) {
		return false, errors.New(NIL_VALUE_IS_NOT_ALLOWED)
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return false, err
	}
	res, err := imap.EncodeInvoke(MapContainsValueCodec, imap.name, valueData)
	if err != nil {
		return false, err
	}
	return res.(bool), err
}
func (imap *MapProxy) Clear() (err error) {
	_, err = imap.EncodeInvoke(MapClearCodec, imap.name)
	return err
}
func (imap *MapProxy) Delete(key interface{}) (err error) {
	if !CheckNotNil(key) {
		return errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return err
	}
	_, err = imap.EncodeInvokeOnKey(MapDeleteCodec, keyData, imap.name, keyData, THREAD_ID)
	return err
}
func (imap *MapProxy) IsEmpty() (empty bool, err error) {
	res, err := imap.EncodeInvoke(MapIsEmptyCodec, imap.name)
	if err != nil {
		return false, err
	}
	return res.(bool), err
}
func (imap *MapProxy) AddIndex(attribute string, ordered bool) (err error) {
	_, err = imap.EncodeInvoke(MapAddIndexCodec, imap.name, &attribute, ordered)
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
	res, err := imap.EncodeInvokeOnKey(MapEvictCodec, keyData, imap.name, keyData, THREAD_ID)
	if err != nil {
		return false, err
	}
	return res.(bool), err
}
func (imap *MapProxy) EvictAll() (err error) {
	_, err = imap.EncodeInvoke(MapEvictAllCodec, imap.name)
	return err
}
func (imap *MapProxy) Flush() (err error) {
	_, err = imap.EncodeInvoke(MapFlushCodec, imap.name)
	return err
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
	lease = GetTimeInMilliSeconds(lease, leaseTimeUnit)
	_, err = imap.EncodeInvokeOnKey(MapLockCodec, keyData, imap.name, keyData, THREAD_ID, lease, imap.client.ProxyManager.nextReferenceId())
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
	res, err := imap.EncodeInvokeOnKey(MapTryLockCodec, keyData, imap.name, keyData, THREAD_ID, lease, timeout, imap.client.ProxyManager.nextReferenceId())
	if err != nil {
		return false, err
	}
	return res.(bool), err
}
func (imap *MapProxy) Unlock(key interface{}) (err error) {
	if !CheckNotNil(key) {
		return errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return err
	}
	_, err = imap.EncodeInvokeOnKey(MapUnlockCodec, keyData, imap.name, keyData, THREAD_ID, imap.client.ProxyManager.nextReferenceId())
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
	_, err = imap.EncodeInvokeOnKey(MapForceUnlockCodec, keyData, imap.name, keyData, THREAD_ID, imap.client.ProxyManager.nextReferenceId())
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
	res, err := imap.EncodeInvokeOnKey(MapIsLockedCodec, keyData, imap.name, keyData)
	if err != nil {
		return false, err
	}
	return res.(bool), err
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
	return imap.EncodeInvokeOnKey(MapReplaceCodec, keyData, imap.name, keyData, valueData, THREAD_ID)
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
	res, err := imap.EncodeInvokeOnKey(MapReplaceIfSameCodec, keyData, imap.name, keyData, oldValueData, newValueData, THREAD_ID)
	if err != nil {
		return false, err
	}
	return res.(bool), err
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
	_, err = imap.EncodeInvokeOnKey(MapSetCodec, keyData, imap.name, keyData, valueData, THREAD_ID, ttl)
	return err
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
	return imap.EncodeInvokeOnKey(MapPutIfAbsentCodec, keyData, imap.name, keyData, valueData, THREAD_ID, TTL)
}
func (imap *MapProxy) PutAll(mp map[interface{}]interface{}) (err error) {
	if len(mp) == 0 {
		return errors.New("empty map is not allowed")
	}
	partitions := make(map[int32][]*Pair)
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
		partitions[partitionId] = append(partitions[partitionId], pair)
	}
	for partitionId, entryList := range partitions {
		_, err = imap.EncodeInvokeOnPartition(MapPutAllCodec, partitionId, imap.name, entryList)
		if err != nil {
			return err
		}
	}
	return nil
}
func (imap *MapProxy) KeySet() (keySet []interface{}, err error) {
	res, err := imap.EncodeInvoke(MapKeySetCodec, imap.name)
	if err != nil {
		return nil, err
	}
	return res.([]interface{}), err

}
func (imap *MapProxy) KeySetWithPredicate(predicate IPredicate) (keySet []interface{}, err error) {
	if predicate == nil {
		return nil, NewHazelcastSerializationError("predicate should not be nil", nil)
	}
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return nil, err
	}
	res, err := imap.EncodeInvoke(MapKeySetWithPredicateCodec, imap.name, predicateData)
	if err != nil {
		return nil, err
	}
	return res.([]interface{}), err
}
func (imap *MapProxy) Values() (values []interface{}, err error) {
	res, err := imap.EncodeInvoke(MapValuesCodec, imap.name)
	if err != nil {
		return nil, err
	}
	return res.([]interface{}), err

}
func (imap *MapProxy) ValuesWithPredicate(predicate IPredicate) (values []interface{}, err error) {
	if predicate == nil {
		return nil, NewHazelcastSerializationError("predicate should not be nil", nil)
	}
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return nil, err
	}
	res, err := imap.EncodeInvoke(MapValuesWithPredicateCodec, imap.name, predicateData)
	if err != nil {
		return nil, err
	}
	return res.([]interface{}), err
}
func (imap *MapProxy) EntrySet() (resultPairs []IPair, err error) {
	res, err := imap.EncodeInvoke(MapEntrySetCodec, imap.name)
	if err != nil {
		return nil, err
	}
	pairs := res.([]*Pair)
	pairSlice := make([]IPair, len(pairs))
	for index, pair := range pairs {
		pairSlice[index] = pair
	}
	return pairSlice, err
}
func (imap *MapProxy) EntrySetWithPredicate(predicate IPredicate) (resultPairs []IPair, err error) {
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return nil, err
	}
	res, err := imap.EncodeInvoke(MapEntriesWithPredicateCodec, imap.name, predicateData)
	if err != nil {
		return nil, err
	}
	pairs := res.([]*Pair)
	pairSlice := make([]IPair, len(pairs))
	for index, pair := range pairs {
		pairSlice[index] = pair
	}
	return pairSlice, err
}
func (imap *MapProxy) GetAll(keys []interface{}) (entryMap map[interface{}]interface{}, err error) {
	if !CheckNotEmpty(keys) {
		return nil, errors.New(NIL_KEYS_ARE_NOT_ALLOWED)
	}
	partitions := make(map[int32][]*serialization.Data)
	entryMap = make(map[interface{}]interface{}, 0)
	for _, key := range keys {
		keyData, err := imap.ToData(key)
		if err != nil {
			return nil, err
		}
		partitionId := imap.client.PartitionService.GetPartitionId(keyData)
		partitions[partitionId] = append(partitions[partitionId], keyData)
	}
	for partitionId, keyList := range partitions {
		res, err := imap.EncodeInvokeOnPartition(MapGetAllCodec, partitionId, imap.name, keyList)
		if err != nil {
			return nil, err
		}

		for _, entry := range res.([]*Pair) {
			entryMap[entry.Key()] = entry.Value()
		}
	}
	return entryMap, nil
}

func (imap *MapProxy) GetEntryView(key interface{}) (entryView IEntryView, err error) {
	if !CheckNotNil(key) {
		return nil, errors.New(NIL_KEY_IS_NOT_ALLOWED)
	}
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	res, err := imap.EncodeInvokeOnKey(MapGetEntryViewCodec, keyData, imap.name, keyData, THREAD_ID)
	if err != nil {
		return nil, err
	}
	entryData := res.(*EntryView)
	resultKey, _ := imap.ToObject(entryData.Key().(*serialization.Data))
	resultValue, _ := imap.ToObject(entryData.Value().(*serialization.Data))

	entryView = NewEntryView(resultKey, resultValue, entryData.Cost(),
		entryData.CreationTime(), entryData.ExpirationTime(), entryData.Hits(), entryData.LastAccessTime(), entryData.LastStoredTime(),
		entryData.LastUpdateTime(), entryData.Version(), entryData.EvictionCriteriaNumber(), entryData.Ttl())
	return entryView, err
}

func (imap *MapProxy) AddEntryListener(listener interface{}, includeValue bool) (registrationID *string, err error) {
	listenerFlags := GetEntryListenerFlags(listener)
	request := MapAddEntryListenerCodec.EncodeRequest(imap.name, includeValue, listenerFlags, imap.isSmart())
	eventHandler := func(clientMessage *ClientMessage) {
		MapAddEntryListenerCodec.Handle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *ClientMessage {
		return MapRemoveEntryListenerCodec.EncodeRequest(imap.name, registrationId)
	}, func(clientMessage *ClientMessage) *string {
		res, err := MapAddEntryListenerCodec.DecodeResponse(clientMessage, nil)
		if err != nil {
			return nil
		}
		return res.(*string)
	})
}
func (imap *MapProxy) AddEntryListenerWithPredicate(listener interface{}, predicate IPredicate, includeValue bool) (*string, error) {
	listenerFlags := GetEntryListenerFlags(listener)
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return nil, err
	}
	request := MapAddEntryListenerWithPredicateCodec.EncodeRequest(imap.name, predicateData, includeValue, listenerFlags, false)
	eventHandler := func(clientMessage *ClientMessage) {
		MapAddEntryListenerWithPredicateCodec.Handle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *ClientMessage {
			return MapRemoveEntryListenerCodec.EncodeRequest(imap.name, registrationId)
		}, func(clientMessage *ClientMessage) *string {
			res, err := MapAddEntryListenerWithPredicateCodec.DecodeResponse(clientMessage, nil)
			if err != nil {
				return nil
			}
			return res.(*string)
		})
}

func (imap *MapProxy) AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID *string, err error) {

	listenerFlags := GetEntryListenerFlags(listener)
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	request := MapAddEntryListenerToKeyCodec.EncodeRequest(imap.name, keyData, includeValue, listenerFlags, imap.isSmart())
	eventHandler := func(clientMessage *ClientMessage) {
		MapAddEntryListenerToKeyCodec.Handle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler, func(registrationId *string) *ClientMessage {
		return MapRemoveEntryListenerCodec.EncodeRequest(imap.name, registrationId)
	}, func(clientMessage *ClientMessage) *string {
		res, err := MapAddEntryListenerToKeyCodec.DecodeResponse(clientMessage, nil)
		if err != nil {
			return nil
		}
		return res.(*string)
	})
}
func (imap *MapProxy) AddEntryListenerToKeyWithPredicate(listener interface{}, predicate IPredicate, key interface{}, includeValue bool) (*string, error) {
	listenerFlags := GetEntryListenerFlags(listener)
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return nil, err
	}
	request := MapAddEntryListenerToKeyWithPredicateCodec.EncodeRequest(imap.name, keyData, predicateData, includeValue, listenerFlags, false)
	eventHandler := func(clientMessage *ClientMessage) {
		MapAddEntryListenerToKeyWithPredicateCodec.Handle(clientMessage, func(key *serialization.Data, oldValue *serialization.Data, value *serialization.Data, mergingValue *serialization.Data, eventType int32, Uuid *string, numberOfAffectedEntries int32) {
			imap.onEntryEvent(key, oldValue, value, mergingValue, eventType, Uuid, numberOfAffectedEntries, includeValue, listener)
		})
	}
	return imap.client.ListenerService.registerListener(request, eventHandler,
		func(registrationId *string) *ClientMessage {
			return MapRemoveEntryListenerCodec.EncodeRequest(imap.name, registrationId)
		}, func(clientMessage *ClientMessage) *string {
			res, err := MapAddEntryListenerToKeyWithPredicateCodec.DecodeResponse(clientMessage, nil)
			if err != nil {
				return nil
			}
			return res.(*string)
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
	return imap.client.ListenerService.deregisterListener(*registrationId, func(registrationId *string) *ClientMessage {
		return MapRemoveEntryListenerCodec.EncodeRequest(imap.name, registrationId)
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
	return imap.EncodeInvokeOnKey(MapExecuteOnKeyCodec, keyData, imap.name, entryProcessorData, keyData, THREAD_ID)
}

func (imap *MapProxy) ExecuteOnKeys(keys []interface{}, entryProcessor interface{}) (keyToResultPairs []IPair, err error) {
	keysData := make([]*serialization.Data, len(keys))
	for index, key := range keys {
		keyData, err := imap.ToData(key)
		if err != nil {
			return nil, err
		}
		keysData[index] = keyData
	}
	entryProcessorData, err := imap.ToData(entryProcessor)
	if err != nil {
		return nil, err
	}
	res, err := imap.EncodeInvoke(MapExecuteOnKeysCodec, imap.name, entryProcessorData, keysData)
	if err != nil {
		return nil, err
	}
	pairs := res.([]*Pair)
	pairSlice := make([]IPair, len(pairs))
	for index, pair := range pairs {
		pairSlice[index] = pair
	}
	return pairSlice, err
}
func (imap *MapProxy) ExecuteOnEntries(entryProcessor interface{}) (keyToResultPairs []IPair, err error) {
	entryProcessorData, err := imap.ToData(entryProcessor)
	if err != nil {
		return nil, err
	}
	res, err := imap.EncodeInvoke(MapExecuteOnAllKeysCodec, imap.name, entryProcessorData)
	if err != nil {
		return nil, err
	}
	pairs := res.([]*Pair)
	pairSlice := make([]IPair, len(pairs))
	for index, pair := range pairs {
		pairSlice[index] = pair
	}
	return pairSlice, err
}

func (imap *MapProxy) ExecuteOnEntriesWithPredicate(entryProcessor interface{}, predicate IPredicate) ([]IPair, error) {
	predicateData, err := imap.ToData(predicate)
	if err != nil {
		return nil, err
	}
	entryProcessorData, err := imap.ToData(entryProcessor)
	if err != nil {
		return nil, err
	}
	res, err := imap.EncodeInvoke(MapExecuteWithPredicateCodec, imap.name, entryProcessorData, predicateData)
	if err != nil {
		return nil, err
	}
	pairs := res.([]*Pair)
	pairSlice := make([]IPair, len(pairs))
	for index, pair := range pairs {
		pairSlice[index] = pair
	}
	return pairSlice, err
}
