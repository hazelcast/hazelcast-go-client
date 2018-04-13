// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package collection

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

func ObjectToDataCollection(objects []interface{}, service *SerializationService) ([]*Data, error) {
	if objects == nil {
		return nil, core.NewHazelcastNilPointerError(common.NilSliceIsNotAllowed, nil)
	}
	elementsData := make([]*Data, len(objects))
	for index, element := range objects {
		if element == nil {
			return nil, core.NewHazelcastNilPointerError(common.NilArgIsNotAllowed, nil)
		}
		elementData, err := service.ToData(element)
		if err != nil {
			return nil, err
		}
		elementsData[index] = elementData
	}
	return elementsData, nil
}

func DataToObjectCollection(dataSlice []*Data, service *SerializationService) ([]interface{}, error) {
	if dataSlice == nil {
		return nil, core.NewHazelcastNilPointerError(common.NilSliceIsNotAllowed, nil)
	}
	elements := make([]interface{}, len(dataSlice))
	for index, data := range dataSlice {
		element, err := service.ToObject(data)
		if err != nil {
			return nil, err
		}
		elements[index] = element
	}
	return elements, nil
}

func DataToObjectPairCollection(dataSlice []*Pair, service *SerializationService) (pairSlice []core.IPair, err error) {
	pairSlice = make([]core.IPair, len(dataSlice))
	for index, pairData := range dataSlice {
		key, err := service.ToObject(pairData.Key().(*Data))
		if err != nil {
			return nil, err
		}
		value, err := service.ToObject(pairData.Value().(*Data))
		if err != nil {
			return nil, err
		}
		pairSlice[index] = core.IPair(NewPair(key, value))
	}
	return
}
