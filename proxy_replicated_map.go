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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	pubser "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type ReplicatedMap struct {
	*proxy
	refIDGenerator *iproxy.ReferenceIDGenerator
	partitionID    int32
	ctx            context.Context
}

func newReplicatedMapImpl(ctx context.Context, p *proxy) (*ReplicatedMap, error) {
	nameData, err := p.validateAndSerialize(p.name)
	if err != nil {
		return nil, err
	}
	partitionID, err := p.partitionService.GetPartitionID(nameData)
	if err != nil {
		panic(fmt.Sprintf("error getting partition id with key: %s", p.name))
	}
	rp := &ReplicatedMap{
		proxy:          p,
		refIDGenerator: iproxy.NewReferenceIDGenerator(),
		partitionID:    partitionID,
		ctx:            ctx,
	}
	return rp, nil
}

func (m *ReplicatedMap) withContext(ctx context.Context) *ReplicatedMap {
	return &ReplicatedMap{
		proxy:          m.proxy,
		refIDGenerator: m.refIDGenerator,
		partitionID:    m.partitionID,
		ctx:            ctx,
	}
}

// AddEntryListener adds a continuous entry listener to this map.
func (m *ReplicatedMap) AddEntryListener(handler EntryNotifiedHandler) (internal.UUID, error) {
	return m.addEntryListener(nil, nil, handler)
}

// AddEntryListenerToKey adds a continuous entry listener to this map.
func (m *ReplicatedMap) AddEntryListenerToKey(key interface{}, handler EntryNotifiedHandler) (internal.UUID, error) {
	return m.addEntryListener(key, nil, handler)
}

// AddEntryListenerWithPredicate adds a continuous entry listener to this map.
func (m *ReplicatedMap) AddEntryListenerWithPredicate(predicate predicate.Predicate, handler EntryNotifiedHandler) (internal.UUID, error) {
	return m.addEntryListener(nil, predicate, handler)
}

// AddEntryListenerToKeyWithPredicate adds a continuous entry listener to this map.
func (m *ReplicatedMap) AddEntryListenerToKeyWithPredicate(key interface{}, predicate predicate.Predicate, handler EntryNotifiedHandler) (internal.UUID, error) {
	return m.addEntryListener(key, predicate, handler)
}

// Clear deletes all entries one by one and fires related events
func (m *ReplicatedMap) Clear() error {
	request := codec.EncodeReplicatedMapClearRequest(m.name)
	_, err := m.invokeOnRandomTarget(m.ctx, request, nil)
	return err
}

// ContainsKey returns true if the map contains an entry with the given key
func (m *ReplicatedMap) ContainsKey(key interface{}) (bool, error) {
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
func (m *ReplicatedMap) ContainsValue(value interface{}) (bool, error) {
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
func (m *ReplicatedMap) Get(key interface{}) (interface{}, error) {
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
func (m *ReplicatedMap) GetEntrySet() ([]types.Entry, error) {
	request := codec.EncodeReplicatedMapEntrySetRequest(m.name)
	if response, err := m.invokeOnPartition(m.ctx, request, m.partitionID); err != nil {
		return nil, err
	} else {
		return m.convertPairsToEntries(codec.DecodeReplicatedMapEntrySetResponse(response))
	}
}

// GetKeySet returns keys contained in this map
func (m *ReplicatedMap) GetKeySet() ([]interface{}, error) {
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
func (m *ReplicatedMap) GetValues() ([]interface{}, error) {
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
func (m *ReplicatedMap) IsEmpty() (bool, error) {
	request := codec.EncodeReplicatedMapIsEmptyRequest(m.name)
	if response, err := m.invokeOnPartition(m.ctx, request, m.partitionID); err != nil {
		return false, err
	} else {
		return codec.DecodeReplicatedMapIsEmptyResponse(response), nil
	}
}

// Put sets the value for the given key and returns the old value.
func (m *ReplicatedMap) Put(key interface{}, value interface{}) (interface{}, error) {
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
func (m *ReplicatedMap) PutAll(keyValuePairs []types.Entry) error {
	f := func(partitionID int32, entries []proto.Pair) cb.Future {
		request := codec.EncodeReplicatedMapPutAllRequest(m.name, entries)
		return m.circuitBreaker.TryContextFuture(m.ctx, func(ctx context.Context, attempt int) (interface{}, error) {
			return m.invokeOnPartitionAsync(request, partitionID).GetWithContext(ctx)
		})
	}
	return m.putAll(keyValuePairs, f)
}

// Remove deletes the value for the given key and returns it.
func (m *ReplicatedMap) Remove(key interface{}) (interface{}, error) {
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

// RemoveEntryListener removes the specified entry listener.
func (m *ReplicatedMap) RemoveEntryListener(subscriptionID internal.UUID) error {
	return m.listenerBinder.Remove(subscriptionID)
}

// Size returns the number of entries in this map.
func (m *ReplicatedMap) Size() (int, error) {
	request := codec.EncodeReplicatedMapSizeRequest(m.name)
	if response, err := m.invokeOnPartition(m.ctx, request, m.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeReplicatedMapSizeResponse(response)), nil
	}
}

func (m *ReplicatedMap) addEntryListener(key interface{}, predicate predicate.Predicate, handler EntryNotifiedHandler) (internal.UUID, error) {
	var err error
	var keyData pubser.Data
	var predicateData pubser.Data
	if key != nil {
		if keyData, err = m.validateAndSerialize(key); err != nil {
			return internal.UUID{}, err
		}
	}
	if predicate != nil {
		if predicateData, err = m.validateAndSerialize(predicate); err != nil {
			return internal.UUID{}, err
		}
	}
	subscriptionID := internal.NewUUID()
	addRequest := m.makeListenerRequest(keyData, predicateData, m.config.ClusterConfig.SmartRouting)
	removeRequest := codec.EncodeReplicatedMapRemoveEntryListenerRequest(m.name, subscriptionID)
	listenerHandler := func(msg *proto.ClientMessage) {
		m.makeListenerDecoder(msg, keyData, predicateData, m.makeEntryNotifiedListenerHandler(handler))
	}
	err = m.listenerBinder.Add(subscriptionID, addRequest, removeRequest, listenerHandler)
	return subscriptionID, err
}

func (m *ReplicatedMap) makeListenerRequest(keyData, predicateData pubser.Data, smart bool) *proto.ClientMessage {
	if keyData != nil {
		if predicateData != nil {
			return codec.EncodeReplicatedMapAddEntryListenerToKeyWithPredicateRequest(m.name, keyData, predicateData, smart)
		} else {
			return codec.EncodeReplicatedMapAddEntryListenerToKeyRequest(m.name, keyData, smart)
		}
	} else if predicateData != nil {
		return codec.EncodeReplicatedMapAddEntryListenerWithPredicateRequest(m.name, predicateData, smart)
	} else {
		return codec.EncodeReplicatedMapAddEntryListenerRequest(m.name, smart)
	}
}

func (m *ReplicatedMap) makeListenerDecoder(msg *proto.ClientMessage, keyData, predicateData pubser.Data, handler entryNotifiedHandler) {
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
}
