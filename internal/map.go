package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
)

const (
	THREAD_ID = 1
	TTL       = 0
)

type MapProxy struct {
	proxy
}

func newMapProxy(client *HazelcastClient, name *string) *MapProxy {
	mapProxy := MapProxy{}
	mapProxy.client = client
	mapProxy.name = name
	return &mapProxy
}

//TODO :: Check if key is nil.
func (imap *MapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return nil, err
	}
	request := MapPutEncodeRequest(*imap.name, *keyData, *valueData, THREAD_ID, TTL)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapPutDecodeResponse(responseMessage).Response
	return imap.ToObject(&responseData)
}

func (imap *MapProxy) Get(key interface{}) (value interface{}, err error) {
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	request := MapGetEncodeRequest(*imap.name, *keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapGetDecodeResponse(responseMessage).Response
	return imap.ToObject(&responseData)
}
func (imap *MapProxy) Remove(key interface{}) (value interface{}, err error) {
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	request := MapRemoveEncodeRequest(*imap.name, *keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapRemoveDecodeResponse(responseMessage).Response
	return imap.ToObject(&responseData)
}
func (imap *MapProxy) Size() (size int32, err error) {
	request := MapSizeEncodeRequest(*imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return -1, err
	}
	response := MapSizeDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) ContainsKey(key interface{}) (found bool, err error) {
	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	request := MapContainsKeyEncodeRequest(*imap.name, *keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	response := MapContainsKeyDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) ContainsValue(value interface{}) (found bool, err error) {
	valueData, err := imap.ToData(value)
	if err != nil {
		return false, err
	}
	request := MapContainsValueEncodeRequest(*imap.name, *valueData)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return false, err
	}
	response := MapContainsValueDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) Clear() (err error) {
	request := MapClearEncodeRequest(*imap.name)
	_, err = imap.InvokeOnRandomTarget(request)
	if err != nil {
		return err
	}
	return nil
}
func (imap *MapProxy) Delete(key interface{}) (err error) {
	keyData, err := imap.ToData(key)
	if err != nil {
		return err
	}
	request := MapDeleteEncodeRequest(*imap.name, *keyData, THREAD_ID)
	_, err = imap.InvokeOnKey(request, keyData)
	if err != nil {
		return err
	}
	return nil
}
func (imap *MapProxy) IsEmpty() (empty bool, err error) {
	request := MapIsEmptyEncodeRequest(*imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return false, err
	}
	response := MapIsEmptyDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) AddIndex(attributes *string, ordered bool) (err error) {
	request := MapAddIndexEncodeRequest(*imap.name, *attributes, ordered)
	_, err = imap.InvokeOnRandomTarget(request)
	if err != nil {
		return err
	}
	return nil
}
func (imap *MapProxy) Evict(key interface{}) (bool, error) {
	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	request := MapEvictEncodeRequest(*imap.name, *keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	response := MapEvictDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) EvictAll() error {
	request := MapEvictAllEncodeRequest(*imap.name)
	_, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return err
	}
	return nil
}
func (imap *MapProxy) Flush() error {
	request := MapFlushEncodeRequest(*imap.name)
	_, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return err
	}
	return nil
}
func (imap *MapProxy) Lock(key interface{}) error {
	keyData, err := imap.ToData(key)
	if err != nil {
		return err
	}
	//TODO :: What should be the reference id ?
	request := MapLockEncodeRequest(*imap.name, *keyData, THREAD_ID, -1, imap.client.ProxyManager.nextReferenceId())
	_, err = imap.InvokeOnKey(request, keyData)
	if err != nil {
		return err
	}
	return nil
}
func (imap *MapProxy) UnLock(key interface{}) error {
	keyData, err := imap.ToData(key)
	if err != nil {
		return err
	}
	//TODO :: What should be the reference id ?
	request := MapUnlockEncodeRequest(*imap.name, *keyData, THREAD_ID, imap.client.ProxyManager.nextReferenceId())
	_, err = imap.InvokeOnKey(request, keyData)
	if err != nil {
		return err
	}
	return nil
}
func (imap *MapProxy) IsLocked(key interface{}) (bool, error) {
	keyData, err := imap.ToData(key)
	if err != nil {
		return false, err
	}
	//TODO :: What should be the reference id ?
	request := MapIsLockedEncodeRequest(*imap.name, *keyData)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	response := MapIsLockedDecodeResponse(responseMessage).Response
	return response, nil
}
func (imap *MapProxy) Replace(key interface{}, value interface{}) (interface{}, error) {
	keyData, err := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return nil, err
	}
	request := MapReplaceEncodeRequest(*imap.name, *keyData, *valueData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return false, err
	}
	responseData := MapReplaceDecodeResponse(responseMessage).Response
	return imap.ToObject(&responseData)
}
