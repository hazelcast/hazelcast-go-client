// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package proxy

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
)

type MapImpl struct {
	*Impl
}

func (mp *MapImpl) Clear() error {
	panic("not implemented")
}

func NewMapImpl(proxy *Impl) *MapImpl {
	return &MapImpl{proxy}
}

func (mp *MapImpl) Put(key interface{}, value interface{}) (interface{}, error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeMapPutRequest(mp.name, keyData, valueData, threadID, ttlUnlimited)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	return codec.DecodeMapPutResponse(responseMessage), nil
}

func (mp *MapImpl) Get(key interface{}) (interface{}, error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeMapGetRequest(mp.name, keyData, threadID)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	response := codec.DecodeMapGetResponse(responseMessage)
	return mp.toObject(response)
}
