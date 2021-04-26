/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hazelcast

import (
	"context"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal/cb"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type ReplicatedMap struct {
	*proxy
	refIDGenerator *iproxy.ReferenceIDGenerator
	partitionID    int32
	ctx            context.Context
}

func NewReplicatedMapImpl(ctx context.Context, p *proxy) *ReplicatedMap {
	partitionID, err := p.partitionService.GetPartitionIDWithKey(p.name)
	if err != nil {
		panic(fmt.Sprintf("error getting partition id with key: %s", p.name))
	}
	return &ReplicatedMap{
		proxy:          p,
		refIDGenerator: iproxy.NewReferenceIDGenerator(),
		partitionID:    partitionID,
		ctx:            ctx,
	}
}

func (m *ReplicatedMap) withContext(ctx context.Context) *ReplicatedMap {
	return &ReplicatedMap{
		proxy:          m.proxy,
		refIDGenerator: m.refIDGenerator,
		partitionID:    m.partitionID,
		ctx:            ctx,
	}
}

// Clear deletes all entries one by one and fires related events
func (m ReplicatedMap) Clear() error {
	request := codec.EncodeReplicatedMapClearRequest(m.name)
	_, err := m.invokeOnRandomTarget(m.ctx, request, nil)
	return err
}

// ContainsKey returns true if the map contains an entry with the given key
func (m ReplicatedMap) ContainsKey(key interface{}) (bool, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec.EncodeReplicatedMapContainsKeyRequest(m.name, keyData)
		if response, err := m.invokeOnKey(m.ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec.DecodeReplicatedMapContainsKeyResponse(response), nil
		}
	}
}

// ContainsValue returns true if the map contains an entry with the given value
func (m ReplicatedMap) ContainsValue(value interface{}) (bool, error) {
	if valueData, err := m.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeReplicatedMapContainsValueRequest(m.name, valueData)
		if response, err := m.invokeOnPartition(m.ctx, request, m.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeReplicatedMapContainsValueResponse(response), nil
		}
	}
}

// Get returns the value for the specified key, or nil if this map does not contain this key.
// Warning:
//   This method returns a clone of original value, modifying the returned value does not change the
//   actual value in the map. One should put modified value back to make changes visible to all nodes.
func (m ReplicatedMap) Get(key interface{}) (interface{}, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapGetRequest(m.name, keyData)
		if response, err := m.invokeOnKey(m.ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeReplicatedMapGetResponse(response))
		}
	}
}

// GetEntrySet returns a clone of the mappings contained in this map.
func (m ReplicatedMap) GetEntrySet() ([]types.Entry, error) {
	request := codec.EncodeReplicatedMapEntrySetRequest(m.name)
	if response, err := m.invokeOnPartition(m.ctx, request, m.partitionID); err != nil {
		return nil, err
	} else {
		return m.convertPairsToEntries(codec.DecodeReplicatedMapEntrySetResponse(response))
	}
}

// GetKeySet returns keys contained in this map
func (m ReplicatedMap) GetKeySet() ([]interface{}, error) {
	request := codec.EncodeReplicatedMapKeySetRequest(m.name)
	if response, err := m.invokeOnPartition(m.ctx, request, m.partitionID); err != nil {
		return nil, err
	} else {
		keyDatas := codec.DecodeReplicatedMapKeySetResponse(response)
		keys := make([]interface{}, len(keyDatas))
		for i, keyData := range keyDatas {
			if key, err := m.convertToObject(keyData); err != nil {
				return nil, err
			} else {
				keys[i] = key
			}
		}
		return keys, nil
	}
}

// GetValues returns a list clone of the values contained in this map
func (m ReplicatedMap) GetValues() ([]interface{}, error) {
	request := codec.EncodeReplicatedMapValuesRequest(m.name)
	if response, err := m.invokeOnPartition(m.ctx, request, m.partitionID); err != nil {
		return nil, err
	} else {
		valueDatas := codec.DecodeReplicatedMapValuesResponse(response)
		values := make([]interface{}, len(valueDatas))
		for i, valueData := range valueDatas {
			if value, err := m.convertToObject(valueData); err != nil {
				return nil, err
			} else {
				values[i] = value
			}
		}
		return values, nil
	}
}

// IsEmpty returns true if this map contains no key-value mappings.
func (m ReplicatedMap) IsEmpty() (bool, error) {
	request := codec.EncodeReplicatedMapIsEmptyRequest(m.name)
	if response, err := m.invokeOnPartition(m.ctx, request, m.partitionID); err != nil {
		return false, err
	} else {
		return codec.DecodeReplicatedMapIsEmptyResponse(response), nil
	}
}

// ListenEntryNotification adds a continuous entry listener to this map.
func (m ReplicatedMap) ListenEntryNotification(handler EntryNotifiedHandler) (string, error) {
	subscriptionID := m.refIDGenerator.NextID()
	if err := m.listenEntryNotified(nil, nil, subscriptionID, handler); err != nil {
		return "", err
	}
	return event.FormatSubscriptionID(subscriptionID), nil
}

// ListenEntryNotification adds a continuous entry listener to this map.
func (m ReplicatedMap) ListenEntryNotificationToKey(key interface{}, subscriptionID int64, handler EntryNotifiedHandler) error {
	return m.listenEntryNotified(key, nil, subscriptionID, handler)
}

// ListenEntryNotification adds a continuous entry listener to this map.
func (m ReplicatedMap) ListenEntryNotificationWithPredicate(predicate predicate.Predicate, handler EntryNotifiedHandler) (string, error) {
	subscriptionID := m.refIDGenerator.NextID()
	if err := m.listenEntryNotified(nil, predicate, subscriptionID, handler); err != nil {
		return "", err
	}
	return event.FormatSubscriptionID(subscriptionID), nil
}

// ListenEntryNotification adds a continuous entry listener to this map.
func (m ReplicatedMap) ListenEntryNotificationToKeyWithPredicate(key interface{}, predicate predicate.Predicate, handler EntryNotifiedHandler) (string, error) {
	subscriptionID := m.refIDGenerator.NextID()
	if err := m.listenEntryNotified(key, predicate, subscriptionID, handler); err != nil {
		return "", err
	}
	return event.FormatSubscriptionID(subscriptionID), nil
}

// Put sets the value for the given key and returns the old value.
func (m ReplicatedMap) Put(key interface{}, value interface{}) (interface{}, error) {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapPutRequest(m.name, keyData, valueData, TtlUnlimited)
		if response, err := m.invokeOnKey(m.ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeReplicatedMapPutResponse(response))
		}
	}
}

// PutAll copies all of the mappings from the specified map to this map.
// No atomicity guarantees are given. In the case of a failure, some of the key-value tuples may get written,
// while others are not.
func (m ReplicatedMap) PutAll(keyValuePairs []types.Entry) error {
	f := func(partitionID int32, entries []proto.Pair) cb.Future {
		return m.circuitBreaker.TryContext(m.ctx, func(ctx context.Context) (interface{}, error) {
			request := codec.EncodeReplicatedMapPutAllRequest(m.name, entries)
			return m.invokeOnPartitionAsync(request, partitionID).GetContext(ctx)
		})
	}
	return m.putAll(keyValuePairs, f)
}

// Remove deletes the value for the given key and returns it.
func (m ReplicatedMap) Remove(key interface{}) (interface{}, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec.EncodeReplicatedMapRemoveRequest(m.name, keyData)
		if response, err := m.invokeOnKey(m.ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec.DecodeReplicatedMapRemoveResponse(response))
		}
	}
}

// Size returns the number of entries in this map.
func (m ReplicatedMap) Size() (int, error) {
	request := codec.EncodeReplicatedMapSizeRequest(m.name)
	if response, err := m.invokeOnPartition(m.ctx, request, m.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeReplicatedMapSizeResponse(response)), nil
	}
}

// UnlistenEntryNotification removes the specified entry listener.
func (m ReplicatedMap) UnlistenEntryNotification(subscriptionID string) error {
	if subscriptionIDInt, err := event.ParseSubscriptionID(subscriptionID); err != nil {
		return fmt.Errorf("invalid subscription ID: %s", subscriptionID)
	} else {
		m.userEventDispatcher.Unsubscribe(EventEntryNotified, subscriptionIDInt)
		return m.listenerBinder.Remove(m.name, subscriptionIDInt)
	}
}

func (m *ReplicatedMap) listenEntryNotified(key interface{}, predicate predicate.Predicate, subscriptionID int64, handler EntryNotifiedHandler) error {
	var request *proto.ClientMessage
	var err error
	var keyData pubserialization.Data
	var predicateData pubserialization.Data
	if key != nil {
		if keyData, err = m.validateAndSerialize(key); err != nil {
			return err
		}
	}
	if predicate != nil {
		if predicateData, err = m.validateAndSerialize(predicate); err != nil {
			return err
		}
	}
	if keyData != nil {
		if predicateData != nil {
			request = codec.EncodeReplicatedMapAddEntryListenerToKeyWithPredicateRequest(m.name, keyData, predicateData, m.smartRouting)
		} else {
			request = codec.EncodeReplicatedMapAddEntryListenerToKeyRequest(m.name, keyData, m.smartRouting)
		}
	} else if predicateData != nil {
		request = codec.EncodeReplicatedMapAddEntryListenerWithPredicateRequest(m.name, predicateData, m.smartRouting)
	} else {
		request = codec.EncodeReplicatedMapAddEntryListenerRequest(m.name, m.smartRouting)
	}
	err = m.listenerBinder.Add(request, subscriptionID, func(msg *proto.ClientMessage) {
		handler := func(binKey pubserialization.Data, binValue pubserialization.Data, binOldValue pubserialization.Data, binMergingValue pubserialization.Data, binEventType int32, binUUID internal.UUID, numberOfAffectedEntries int32) {
			key := m.mustConvertToInterface(binKey, "invalid key at ListenEntryNotification")
			value := m.mustConvertToInterface(binValue, "invalid value at ListenEntryNotification")
			oldValue := m.mustConvertToInterface(binOldValue, "invalid oldValue at ListenEntryNotification")
			mergingValue := m.mustConvertToInterface(binMergingValue, "invalid mergingValue at ListenEntryNotification")
			m.userEventDispatcher.Publish(newEntryNotifiedEventImpl(m.name, binUUID.String(), key, value, oldValue, mergingValue, int(numberOfAffectedEntries)))
		}
		if keyData != nil {
			if predicateData != nil {
				codec.HandleReplicatedMapAddEntryListenerToKeyWithPredicate(msg, handler)
			} else {
				codec.HandleReplicatedMapAddEntryListenerToKey(msg, handler)
			}
		} else if predicateData != nil {
			codec.HandleReplicatedMapAddEntryListenerWithPredicate(msg, handler)
		} else {
			codec.HandleReplicatedMapAddEntryListener(msg, handler)
		}
	})
	if err != nil {
		return err
	}
	m.userEventDispatcher.Subscribe(EventEntryNotified, subscriptionID, func(event event.Event) {
		if entryNotifiedEvent, ok := event.(*EntryNotified); ok {
			if entryNotifiedEvent.OwnerName == m.name {
				handler(entryNotifiedEvent)
			}
		} else {
			panic("cannot cast event to hztypes.EntryNotified event")
		}
	})
	return nil
}
