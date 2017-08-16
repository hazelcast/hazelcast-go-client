package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"
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
	/*
		keyData, err  := imap.ToData(key)
		if err != nil {
			return nil, err
		}
		valueData, err := imap.ToData(value)
		if err != nil {
			return nil, err
		}
	*/
	keyData := &serialization.Data{[]byte(key.(string))}
	valueData := &serialization.Data{[]byte(value.(string))}
	request := MapPutEncodeRequest(*imap.name, *keyData, *valueData, THREAD_ID, TTL)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapPutDecodeResponse(responseMessage).Response
	//return imap.ToObject(&responseData)
	return &responseData, nil
}

func (imap *MapProxy) Get(key interface{}) (value interface{}, err error) {
	keyData := &serialization.Data{[]byte(key.(string))}
	request := MapGetEncodeRequest(*imap.name, *keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapGetDecodeResponse(responseMessage).Response
	return &responseData, nil
}
func (imap *MapProxy) Remove(key interface{}) (value interface{}, err error) {
	keyData := &serialization.Data{[]byte(key.(string))}
	request := MapRemoveEncodeRequest(*imap.name, *keyData, THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapRemoveDecodeResponse(responseMessage).Response
	return &responseData, nil
}
func (imap *MapProxy) Size() (size interface{}, err error) {
	request := MapSizeEncodeRequest(*imap.name)
	responseMessage, err := imap.InvokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}
	responseData := MapSizeDecodeResponse(responseMessage).Response
	return responseData, nil
}
func (imap *MapProxy) ContainsKey(key interface{}) (found interface{}, err error) {
	//TODO Implement this
	return nil, nil
}
