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
	"github.com/hazelcast/hazelcast-go-client/internal/util/validationutil"
	"github.com/hazelcast/hazelcast-go-client/types"
)

/*
List is a concurrent, distributed, ordered collection. The user of this
data structure has precise control over where in the list each element is
inserted. The user can access elements by their integer index (position in the list),
and search for elements in the list.

List is not a partitioned Hazelcast data structure. So all the contents of the List are stored in a single
machine (and in the backup). So, a single List will not scale by adding more members in the cluster.
*/
type List struct {
	*proxy
	partitionID int32
}

func newList(p *proxy) (*List, error) {
	if partitionID, err := p.stringToPartitionID(p.name); err != nil {
		return nil, err
	} else {
		return &List{proxy: p, partitionID: partitionID}, nil
	}
}

// AddListener adds an item listener for this list.
// The listener will be invoked whenever an item is added to or removed from this list.
// Returns subscription ID of the listener.
func (l *List) AddListener(handler ListItemNotifiedHandler) (types.UUID, error) {
	return l.addListener(false, handler)
}

// AddListener adds an item listener for this list.
// The listener will be invoked whenever an item is added to or removed from this list.
// Received events include the updated item.
// Returns subscription ID of the listener.
func (l *List) AddListenerIncludeValue(handler ListItemNotifiedHandler) (types.UUID, error) {
	return l.addListener(true, handler)
}

func (l *List) addListener(includeValue bool, handler ListItemNotifiedHandler) (types.UUID, error) {
	subscriptionID := types.NewUUID()
	addRequest := codec.EncodeListAddListenerRequest(l.name, includeValue, l.config.ClusterConfig.SmartRouting)
	removeRequest := codec.EncodeListRemoveListenerRequest(l.name, subscriptionID)
	listenerHandler := func(msg *proto.ClientMessage) {
		codec.HandleListAddListener(msg, func(itemData *iserialization.Data, uuid types.UUID, eventType int32) {
			item, err := l.convertToObject(itemData)
			if err != nil {
				l.logger.Warnf("cannot convert data to Go value: %v", err)
				return
			}
			member := l.clusterService.GetMemberByUUID(uuid)
			handler(newListItemNotified(l.name, item, member, eventType))
		})
	}
	err := l.listenerBinder.Add(subscriptionID, addRequest, removeRequest, listenerHandler)
	return subscriptionID, err
}

// RemoveListener removes the item listener with the given subscription ID.
func (l *List) RemoveListener(subscriptionID types.UUID) error {
	return l.listenerBinder.Remove(subscriptionID)
}

// Add appends the specified element to the end of this list.
// Returns true if the list has changed as a result of this operation, false otherwise.
func (l *List) Add(element interface{}) (bool, error) {
	elementData, err := l.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := codec.EncodeListAddRequest(l.name, elementData)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return false, err
	}
	return codec.DecodeListAddResponse(response), nil
}

// AddAt inserts the specified element at the specified index.
// Shifts the subsequent elements to the right.
func (l *List) AddAt(index int, element interface{}) error {
	indexAsInt32, err := validationutil.ValidateAsNonNegativeInt32(index)
	if err != nil {
		return err
	}
	elementData, err := l.validateAndSerialize(element)
	if err != nil {
		return err
	}
	request := codec.EncodeListAddWithIndexRequest(l.name, indexAsInt32, elementData)
	_, err = l.invokeOnPartition(context.TODO(), request, l.partitionID)
	return err
}

// AddAll appends all elements in the specified slice to the end of this list.
// Returns true if the list has changed as a result of this operation, false otherwise.
func (l *List) AddAll(elements ...interface{}) (bool, error) {
	elementsData, err := l.validateAndSerializeValues(elements...)
	if err != nil {
		return false, err
	}
	request := codec.EncodeListAddAllRequest(l.name, elementsData)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return false, err
	}
	return codec.DecodeListAddAllResponse(response), nil
}

// AddAllAt inserts all elements in the specified slice at specified index, keeping the order of the slice.
// Shifts the subsequent elements to the right.
// Returns true if the list has changed as a result of this operation, false otherwise.
func (l *List) AddAllAt(index int, elements ...interface{}) (bool, error) {
	indexAsInt32, err := validationutil.ValidateAsNonNegativeInt32(index)
	if err != nil {
		return false, err
	}
	elementsData, err := l.validateAndSerializeValues(elements...)
	if err != nil {
		return false, err
	}
	request := codec.EncodeListAddAllWithIndexRequest(l.name, indexAsInt32, elementsData)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return false, err
	}
	return codec.DecodeListAddAllWithIndexResponse(response), nil
}

// Clear removes all elements from the list.
func (l *List) Clear() error {
	request := codec.EncodeListClearRequest(l.name)
	_, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	return err
}

// Contains checks if the list contains the given element.
// Returns true if the list contains the element, false otherwise.
func (l *List) Contains(element interface{}) (bool, error) {
	elementData, err := l.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := codec.EncodeListContainsRequest(l.name, elementData)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return false, err
	}
	return codec.DecodeListContainsResponse(response), nil
}

// ContainsAll checks if the list contains all of the given elements.
// Returns true if the list contains all of the elements, otherwise false.
func (l *List) ContainsAll(elements ...interface{}) (bool, error) {
	elementsData, err := l.validateAndSerializeValues(elements...)
	if err != nil {
		return false, err
	}
	request := codec.EncodeListContainsAllRequest(l.name, elementsData)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return false, err
	}
	return codec.DecodeListContainsAllResponse(response), nil
}

// Get retrieves the element at given index.
func (l *List) Get(index int) (interface{}, error) {
	indexAsInt32, err := validationutil.ValidateAsNonNegativeInt32(index)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeListGetRequest(l.name, indexAsInt32)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return nil, err
	}
	return l.convertToObject(codec.DecodeListGetResponse(response))
}

// IndexOf returns the index of the first occurrence of the given element in this list.
func (l *List) IndexOf(element interface{}) (int, error) {
	elementData, err := l.validateAndSerialize(element)
	if err != nil {
		return 0, err
	}
	request := codec.EncodeListIndexOfRequest(l.name, elementData)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return 0, err
	}
	return int(codec.DecodeListIndexOfResponse(response)), nil
}

// IsEmpty return true if the list is empty, false otherwise.
func (l *List) IsEmpty() (bool, error) {
	request := codec.EncodeListIsEmptyRequest(l.name)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return false, err
	}
	return codec.DecodeListIsEmptyResponse(response), nil
}

// LastIndexOf returns the index of the last occurrence of the given element in this list.
func (l *List) LastIndexOf(element interface{}) (int, error) {
	elementData, err := l.validateAndSerialize(element)
	if err != nil {
		return 0, err
	}
	request := codec.EncodeListLastIndexOfRequest(l.name, elementData)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return 0, err
	}
	return int(codec.DecodeListLastIndexOfResponse(response)), nil
}

// Remove removes the given element from this list.
// Returns true if the list has changed as the result of this operation, false otherwise.
func (l *List) Remove(element interface{}) (bool, error) {
	elementData, err := l.validateAndSerialize(element)
	if err != nil {
		return false, err
	}
	request := codec.EncodeListRemoveRequest(l.name, elementData)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return false, err
	}
	return codec.DecodeListRemoveResponse(response), nil
}

// RemoveAt removes the element at the given index.
// Returns the removed element.
func (l *List) RemoveAt(index int) (interface{}, error) {
	indexAsInt32, err := validationutil.ValidateAsNonNegativeInt32(index)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeListRemoveWithIndexRequest(l.name, indexAsInt32)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return nil, err
	}
	return l.convertToObject(codec.DecodeListRemoveWithIndexResponse(response))
}

// RemoveAll removes the given elements from the list.
// Returns true if the list has changed as the result of this operation, false otherwise.
func (l *List) RemoveAll(elements ...interface{}) (bool, error) {
	elementsData, err := l.validateAndSerializeValues(elements...)
	if err != nil {
		return false, err
	}
	request := codec.EncodeListCompareAndRemoveAllRequest(l.name, elementsData)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return false, err
	}
	return codec.DecodeListCompareAndRemoveAllResponse(response), nil
}

// RetainAll removes all elements from this list except the ones contained in the given slice.
// Returns true if the list has changed as a result of this operation, false otherwise.
func (l *List) RetainAll(elements ...interface{}) (bool, error) {
	elementsData, err := l.validateAndSerializeValues(elements...)
	if err != nil {
		return false, err
	}
	request := codec.EncodeListCompareAndRetainAllRequest(l.name, elementsData)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return false, err
	}
	return codec.DecodeListCompareAndRetainAllResponse(response), nil
}

// Set replaces the element at the specified index in this list with the specified element.
// Returns the previous element from the list.
func (l *List) Set(index int, element interface{}) (interface{}, error) {
	indexAsInt32, err := validationutil.ValidateAsNonNegativeInt32(index)
	if err != nil {
		return nil, err
	}
	elementData, err := l.validateAndSerialize(element)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeListSetRequest(l.name, indexAsInt32, elementData)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return nil, err
	}
	return l.convertToObject(codec.DecodeListSetResponse(response))
}

// Size returns the number of elements in this list.
func (l *List) Size() (int, error) {
	request := codec.EncodeListSizeRequest(l.name)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return 0, err
	}
	return int(codec.DecodeListSizeResponse(response)), nil
}

// SubList returns a view of this list that contains elements between index numbers
// from start (inclusive) to end (exclusive).
func (l *List) SubList(start int, end int) ([]interface{}, error) {
	startAsInt32, err := validationutil.ValidateAsNonNegativeInt32(start)
	if err != nil {
		return nil, err
	}
	endAsInt32, err := validationutil.ValidateAsNonNegativeInt32(end)
	if err != nil {
		return nil, err
	}
	request := codec.EncodeListSubRequest(l.name, startAsInt32, endAsInt32)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return nil, err
	}
	return l.convertToObjects(codec.DecodeListSubResponse(response))
}

// ToSlice returns a slice that contains all elements of this list in proper sequence.
func (l *List) ToSlice() ([]interface{}, error) {
	request := codec.EncodeListGetAllRequest(l.name)
	response, err := l.invokeOnPartition(context.TODO(), request, l.partitionID)
	if err != nil {
		return nil, err
	}
	return l.convertToObjects(codec.DecodeListGetAllResponse(response))
}
