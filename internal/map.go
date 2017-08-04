package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
)
const(
	THREAD_ID = 1
	TTL = 0
)
type MapProxy struct {
	proxy
}

func (imap *MapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, err  := imap.ToData(key)
	if err != nil {
		return nil, err
	}
	valueData, err := imap.ToData(value)
	if err != nil {
		return nil, err
	}

	request := MapPutEncodeRequest(imap.name, *keyData, *valueData, THREAD_ID, TTL)
	responseMessage, err := imap.InvokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	responseData := MapPutDecodeResponse(responseMessage).Response
	return imap.ToObject(&responseData)
}

func (imap *MapProxy) Get(key interface{}) (value interface{}, err error) {
	return nil, nil
}
