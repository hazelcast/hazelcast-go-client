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
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
	"reflect"
)

const MapServiceName = "hz:impl:mapService"

type MapImpl struct {
	*Impl
}

func (m *MapImpl) Clear() error {
	panic("not implemented")
}

func NewMapImpl(proxy *Impl) *MapImpl {
	return &MapImpl{proxy}
}

func (m *MapImpl) Put(key interface{}, value interface{}) (interface{}, error) {
	keyData, valueData, err := m.validateAndSerialize2(key, value)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeMapPutRequest(m.name, keyData, valueData, threadID, ttlUnlimited)
	responseMessage, err := m.invokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	return codec.DecodeMapPutResponse(responseMessage), nil
}

func (m *MapImpl) Get(key interface{}) (interface{}, error) {
	keyData, err := m.validateAndSerialize(key)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeMapGetRequest(m.name, keyData, threadID)
	responseMessage, err := m.invokeOnKey(request, keyData)
	if err != nil {
		return nil, err
	}
	response := codec.DecodeMapGetResponse(responseMessage)
	return m.toObject(response)
}

func (m *MapImpl) ListenEntryNotified(flags int32, includeValue bool, handler hztypes.EntryNotifiedHandler) error {
	request := codec.EncodeMapAddEntryListenerRequest(m.name, includeValue, flags, m.smartRouting)
	err := m.listenerBinder.Add(request, func(msg *proto.ClientMessage) {
		//if msg.Type() == bufutil.EventEntry {
		binKey, binValue, binOldValue, binMergingValue, _, uuid, _ := codec.HandleMapAddEntryListener(msg)
		key := m.mustToInterface(binKey, "invalid key at ListenEntryNotified")
		value := m.mustToInterface(binValue, "invalid value at ListenEntryNotified")
		oldValue := m.mustToInterface(binOldValue, "invalid oldValue at ListenEntryNotified")
		mergingValue := m.mustToInterface(binMergingValue, "invalid mergingValue at ListenEntryNotified")
		//numberOfAffectedEntries := m.mustToInterface(binNumberofAffectedEntries, "invalid numberOfAffectedEntries at ListenEntryNotified")
		m.eventDispatcher.Publish(NewEntryNotifiedEventImpl(m.name, "FIX-ME:"+uuid.String(), key, value, oldValue, mergingValue))
		//}
	})
	if err != nil {
		return err
	}
	// derive subscriptionID from the handler
	subscriptionID := int(reflect.ValueOf(handler).Pointer())
	m.eventDispatcher.Subscribe(EventEntryNotified, subscriptionID, func(event event.Event) {
		if entryAddedEvent, ok := event.(hztypes.EntryNotifiedEvent); ok {
			if entryAddedEvent.OwnerName() == m.name {
				handler(entryAddedEvent)
			}
		} else {
			panic("cannot cast event to hztypes.EntryNotifiedEvent event")
		}
	})
	return nil
}
