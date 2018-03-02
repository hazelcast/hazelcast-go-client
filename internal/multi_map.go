package internal

import (
	. "github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"time"
)

type MultiMapProxy struct {
	*proxy
}

func newMultiMapProxy(client *HazelcastClient, serviceName *string, name *string) *MultiMapProxy {
	return &MultiMapProxy{&proxy{client, serviceName, name}}
}

func (mmp *MultiMapProxy) Put(key interface{}, value interface{}) (increased bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := MultiMapPutEncodeRequest(mmp.name, keyData, valueData, ThreadId)
	responseMessage, err := mmp.InvokeOnKey(request, keyData)
	if err != nil {
		return
	}
	responseData := MapPutDecodeResponse(responseMessage).Response
	obj, err := mmp.ToObject(responseData)
	return obj.(bool), err
}

func (mmp *MultiMapProxy) Get(key interface{}) (values []interface{}, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := MultiMapGetEncodeRequest(mmp.name, keyData, ThreadId)
	responseMessage, err := mmp.InvokeOnKey(request, keyData)
	if err != nil {
		return
	}
	responseData := MultiMapValuesDecodeResponse(responseMessage).Response
	values = make([]interface{}, len(*responseData))
	for index, valueData := range *responseData {
		value, err := mmp.ToObject(&valueData)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return
}

func (mmp *MultiMapProxy) Remove(key interface{}, value interface{}) (removed bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := MultiMapRemoveEntryEncodeRequest(mmp.name, keyData, valueData, ThreadId)
	responseMessage, err := mmp.InvokeOnKey(request, keyData)
	if err != nil {
		return
	}
	removed = MultiMapRemoveEntryDecodeResponse(responseMessage).Response
	return
}

func (mmp *MultiMapProxy) RemoveAll(key interface{}) (oldValues []interface{}, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := MultiMapRemoveEncodeRequest(mmp.name, keyData, ThreadId)
	responseMessage, err := mmp.InvokeOnKey(request, keyData)
	if err != nil {
		return
	}
	responseData := MultiMapRemoveDecodeResponse(responseMessage).Response
	oldValues = make([]interface{}, len(*responseData))
	for index, valueData := range *responseData {
		value, err := mmp.ToObject(&valueData)
		if err != nil {
			return nil, err
		}
		oldValues[index] = value
	}
	return
}

func (mmp *MultiMapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := MultiMapContainsKeyEncodeRequest(mmp.name, keyData, ThreadId)
	responseMessage, err := mmp.InvokeOnKey(request, keyData)
	if err != nil {
		return
	}
	found = MultiMapContainsKeyDecodeResponse(responseMessage).Response
	return
}

func (mmp *MultiMapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := mmp.validateAndSerialize(value)
	if err != nil {
		return
	}
	request := MultiMapContainsValueEncodeRequest(mmp.name, valueData)
	responseMessage, err := mmp.InvokeOnRandomTarget(request)
	if err != nil {
		return
	}
	found = MultiMapContainsValueDecodeResponse(responseMessage).Response
	return
}

func (mmp *MultiMapProxy) ContainsEntry(key interface{}, value interface{}) (found bool, err error) {
	keyData, valueData, err := mmp.validateAndSerialize2(key, value)
	if err != nil {
		return
	}
	request := MultiMapContainsEntryEncodeRequest(mmp.name, keyData, valueData, ThreadId)
	responseMessage, err := mmp.InvokeOnKey(request, keyData)
	if err != nil {
		return
	}
	found = MultiMapContainsEntryDecodeResponse(responseMessage).Response
	return
}

func (mmp *MultiMapProxy) Clear() (err error) {
	request := MultiMapClearEncodeRequest(mmp.name)
	_, err = mmp.InvokeOnRandomTarget(request)
	return
}

func (mmp *MultiMapProxy) Delete(key interface{}) (err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := MultiMapDeleteEncodeRequest(mmp.name, keyData, ThreadId)
	_, err = mmp.InvokeOnKey(request, keyData)
	return
}

func (mmp *MultiMapProxy) Size() (size int32, err error) {
	request := MultiMapSizeEncodeRequest(mmp.name)
	responseMessage, err := mmp.InvokeOnRandomTarget(request)
	if err != nil {
		return
	}
	size = MultiMapSizeDecodeResponse(responseMessage).Response
	return
}

func (mmp *MultiMapProxy) ValueCount(key interface{}) (valueCount int32, err error) {
	keyData, err := mmp.validateAndSerialize(key)
	if err != nil {
		return
	}
	request := MultiMapSizeEncodeRequest(mmp.name)
	responseMessage, err := mmp.InvokeOnKey(request, keyData)
	if err != nil {
		return
	}
	valueCount = MultiMapSizeDecodeResponse(responseMessage).Response
	return
}

func (mmp *MultiMapProxy) Values() (values []interface{}, err error) {
	request := MultiMapValuesEncodeRequest(mmp.name)
	responseMessage, err := mmp.InvokeOnRandomTarget(request)
	if err != nil {
		return
	}
	response := MultiMapValuesDecodeResponse(responseMessage).Response
	values = make([]interface{}, len(*response))
	for index, valueData := range *response {
		value, err := mmp.ToObject(&valueData)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return
}

func (mmp *MultiMapProxy) KeySet() (keySet []interface{}, err error) {
	request := MultiMapKeySetEncodeRequest(mmp.name)
	responseMessage, err := mmp.InvokeOnRandomTarget(request)
	if err != nil {
		return
	}
	response := MultiMapKeySetDecodeResponse(responseMessage).Response
	keySet = make([]interface{}, len(*response))
	for index, keyData := range *response {
		key, err := mmp.ToObject(&keyData)
		if err != nil {
			return nil, err
		}
		keySet[index] = key
	}
	return
}

func (mmp *MultiMapProxy) EntrySet() (resultPairs []IPair, err error) {
	request := MultiMapEntrySetEncodeRequest(mmp.name)
	responseMessage, err := mmp.InvokeOnRandomTarget(request)
	if err != nil {
		return
	}
	response := MultiMapEntrySetDecodeResponse(responseMessage).Response
	resultPairs = make([]IPair, len(*response))
	for index, pairData := range *response {
		key, err := mmp.ToObject(pairData.Key().(*Data))
		if err != nil {
			return nil, err
		}
		value, err := mmp.ToObject(pairData.Value().(*Data))
		if err != nil {
			return nil, err
		}
		resultPairs[index] = IPair(NewPair(key, value))
	}
	return
}

func (mmp *MultiMapProxy) AddEntryListener(listener interface{}) (registrationID *string, err error) {
	return
}

func (mmp *MultiMapProxy) AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID *string, err error) {
	return
}

func (mmp *MultiMapProxy) RemoveEntryListener(registrationId *string) (removed bool, err error) {
	return
}

func (mmp *MultiMapProxy) Lock(key interface{}) (err error) {
	return
}

func (mmp *MultiMapProxy) LockWithLeaseTime(key interface{}, lease int64, leaseTimeUnit time.Duration) (err error) {
	return
}

func (mmp *MultiMapProxy) IsLocked(key interface{}) (locked bool, err error) {
	return
}

func (mmp *MultiMapProxy) TryLock(key interface{}) (locked bool, err error) {
	return
}

func (mmp *MultiMapProxy) TryLockWithTimeout(key interface{}, timeout int64, timeoutTimeUnit time.Duration) (locked bool, err error) {
	return
}

func (mmp *MultiMapProxy) TryLockWithTimeoutAndLease(key interface{}, timeout int64, timeoutTimeUnit time.Duration, lease int64, leaseTimeUnit time.Duration) (locked bool, err error) {
	return
}

func (mmp *MultiMapProxy) Unlock(key interface{}) (err error) {
	return
}

func (mmp *MultiMapProxy) ForceUnlock(key interface{}) (err error) {
	return
}
