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

package internal

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
)

type mapProxy struct {
	*proxy
}

func newMapProxy(client *HazelcastClient, serviceName string, name string) *mapProxy {
	return &mapProxy{&proxy{client, serviceName, name}}
}

func (mp *mapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	keyData, valueData, err := mp.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := codec.MapPutCodec.EncodeRequest(mp.name, keyData, valueData, threadID, ttlUnlimited)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	return codec.MapPutCodec.DecodeResponse(responseMessage), nil
}

func (mp *mapProxy) Get(key interface{}) (value interface{}, err error) {
	keyData, err := mp.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := codec.MapGetCodec.EncodeRequest(mp.name, keyData, threadID)
	responseMessage, err := mp.invokeOnKey(request, keyData)
	response := codec.MapGetCodec.DecodeResponse(responseMessage)
	return mp.toObject(response)
}
