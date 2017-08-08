package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"
)
const(
	THREAD_ID = 1
	TTL = 0
)
type MapProxy struct {
	proxy
}

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
	keyData := &serialization.Data{[]byte("successful")}
	valueData := &serialization.Data{[]byte("HEISENBERG")}
	request := MapPutEncodeRequest(imap.name, *keyData, *valueData, THREAD_ID, TTL)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapPutDecodeResponse(responseMessage).Response
	//return imap.ToObject(&responseData)
	return &responseData,nil
}

func (imap *MapProxy) Get(key interface{}) (value interface{}, err error) {
	keyData := &serialization.Data{[]byte("successful")}
	request := MapGetEncodeRequest(imap.name,*keyData,THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapGetDecodeResponse(responseMessage).Response
	return &responseData,nil

	return nil, nil
}
func (imap *MapProxy) Remove(key interface{}) (value interface{},err error){
	keyData := &serialization.Data{[]byte("successful")}
	request := MapRemoveEncodeRequest(imap.name,*keyData,THREAD_ID)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapRemoveDecodeResponse(responseMessage).Response
	return &responseData,nil
	return nil, nil
}
