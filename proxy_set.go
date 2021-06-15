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

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

/* Set is a concurrent, distributed set implementation.

Hazelcast Set is a distributed set which does not allow duplicate elements.
For details, see the Set section in the Hazelcast Reference Manual: https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#set
*/
type Set struct {
	*proxy
	partitionID int32
}

func newSet(p *proxy) (*Set, error) {
	if partitionID, err := p.stringToPartitionID(p.name); err != nil {
		return nil, err
	} else {
		return &Set{proxy: p, partitionID: partitionID}, nil
	}
}

// Add adds the given item to the set.
// Returns true if the item was not already in the set.
func (s *Set) Add(ctx context.Context, item interface{}) (bool, error) {
	if itemData, err := s.validateAndSerialize(item); err != nil {
		return false, err
	} else {
		request := codec.EncodeSetAddRequest(s.name, itemData)
		if resp, err := s.invokeOnPartition(ctx, request, s.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeSetAddResponse(resp), nil
		}
	}
}

// AddAll adds the elements in the specified collection to this set.
// Returns true if the set is changed after the call.
func (s *Set) AddAll(ctx context.Context, values ...interface{}) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if valuesData, err := s.validateAndSerializeValues(values); err != nil {
		return false, err
	} else {
		request := codec.EncodeSetAddAllRequest(s.name, valuesData)
		if response, err := s.invokeOnPartition(ctx, request, s.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeSetAddAllResponse(response), nil
		}
	}
}

// AddListener adds an item listener for this set.
// Listener will be notified for all set add/remove events.
func (s *Set) AddListener(ctx context.Context, handler SetItemNotifiedHandler) (types.UUID, error) {
	return s.addListener(ctx, false, handler)
}

// AddListenerIncludeValue adds an item listener for this set.
// Listener will be notified for all set add/remove events.
// Received events include the updated item.
func (s *Set) AddListenerIncludeValue(ctx context.Context, handler SetItemNotifiedHandler) (types.UUID, error) {
	return s.addListener(ctx, true, handler)
}

// Clear clears this set.
// Set will be empty after this call.
func (s *Set) Clear(ctx context.Context) error {
	request := codec.EncodeSetClearRequest(s.name)
	_, err := s.invokeOnPartition(ctx, request, s.partitionID)
	return err
}

// Contains returns true if the set includes the given value.
func (s *Set) Contains(ctx context.Context, value interface{}) (bool, error) {
	if valueData, err := s.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeSetContainsRequest(s.name, valueData)
		if response, err := s.invokeOnPartition(ctx, request, s.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeSetContainsResponse(response), nil
		}
	}
}

// ContainsAll returns true if the set includes all given values.
func (s *Set) ContainsAll(ctx context.Context, values ...interface{}) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if valuesData, err := s.validateAndSerializeValues(values); err != nil {
		return false, err
	} else {
		request := codec.EncodeSetContainsAllRequest(s.name, valuesData)
		if response, err := s.invokeOnPartition(ctx, request, s.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeSetContainsAllResponse(response), nil
		}
	}
}

// GetAll returns the entries for the given keys.
func (s *Set) GetAll(ctx context.Context) ([]interface{}, error) {
	request := codec.EncodeSetGetAllRequest(s.name)
	if response, err := s.invokeOnPartition(ctx, request, s.partitionID); err != nil {
		return nil, err
	} else {
		return s.convertToObjects(codec.DecodeSetGetAllResponse(response))
	}
}

// IsEmpty returns true if the set is empty.
func (s *Set) IsEmpty(ctx context.Context) (bool, error) {
	request := codec.EncodeSetIsEmptyRequest(s.name)
	if response, err := s.invokeOnPartition(ctx, request, s.partitionID); err != nil {
		return false, err
	} else {
		return codec.DecodeSetIsEmptyResponse(response), nil
	}
}

// Remove removes the specified element from the set if it exists.
func (s *Set) Remove(ctx context.Context, value interface{}) (bool, error) {
	if data, err := s.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec.EncodeSetRemoveRequest(s.name, data)
		if response, err := s.invokeOnPartition(ctx, request, s.partitionID); err != nil {
			return false, nil
		} else {
			return codec.DecodeSetRemoveResponse(response), nil
		}
	}
}

// RemoveAll removes all of the elements of the specified collection from this set.
// Returns true if the set was changed.
func (s *Set) RemoveAll(ctx context.Context, values ...interface{}) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if valuesData, err := s.validateAndSerializeValues(values); err != nil {
		return false, err
	} else {
		request := codec.EncodeSetCompareAndRemoveAllRequest(s.name, valuesData)
		if response, err := s.invokeOnPartition(ctx, request, s.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeSetCompareAndRemoveAllResponse(response), nil
		}
	}
}

// RemoveListener removes the specified listener.
func (s *Set) RemoveListener(ctx context.Context, subscriptionID types.UUID) error {
	return s.listenerBinder.Remove(ctx, subscriptionID)
}

// RetainAll removes the items which are not contained in the specified collection.
// Returns true if the set was changed.
func (s *Set) RetainAll(ctx context.Context, values ...interface{}) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if valuesData, err := s.validateAndSerializeValues(values); err != nil {
		return false, err
	} else {
		request := codec.EncodeSetCompareAndRetainAllRequest(s.name, valuesData)
		if response, err := s.invokeOnPartition(ctx, request, s.partitionID); err != nil {
			return false, err
		} else {
			return codec.DecodeSetCompareAndRetainAllResponse(response), nil
		}
	}
}

// Size returns the number of elements in this set.
func (s *Set) Size(ctx context.Context) (int, error) {
	request := codec.EncodeSetSizeRequest(s.name)
	if response, err := s.invokeOnPartition(ctx, request, s.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec.DecodeSetSizeResponse(response)), nil
	}
}

func (s *Set) addListener(ctx context.Context, includeValue bool, handler SetItemNotifiedHandler) (types.UUID, error) {
	subscriptionID := types.NewUUID()
	addRequest := codec.EncodeSetAddListenerRequest(s.name, includeValue, s.config.ClusterConfig.SmartRouting)
	removeRequest := codec.EncodeSetRemoveListenerRequest(s.name, subscriptionID)
	listenerHandler := func(msg *proto.ClientMessage) {
		codec.HandleSetAddListener(msg, func(itemData *iserialization.Data, uuid types.UUID, eventType int32) {
			if item, err := s.convertToObject(itemData); err != nil {
				s.logger.Warnf("cannot convert data to Go value")
			} else {
				member := s.clusterService.GetMemberByUUID(uuid)
				handler(newSetItemNotified(s.name, item, *member, eventType))
			}
		})
	}
	err := s.listenerBinder.Add(ctx, subscriptionID, addRequest, removeRequest, listenerHandler)
	return subscriptionID, err
}
