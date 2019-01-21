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

package colutil

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/stretchr/testify/assert"
)

func TestColUtilsNilArgument(t *testing.T) {
	_, err := ObjectToDataCollection(nil, nil)
	assert.Error(t, err)

	_, err = DataToObjectCollection(nil, nil)
	assert.Error(t, err)
}

func TestObjectToDataCollectionNonSerializableKey(t *testing.T) {
	service, _ := spi.NewSerializationService(serialization.NewConfig())

	_, err := ObjectToDataCollection(test.NewNonSerializableObjectSlice(), service)
	assert.Error(t, err)
}

func TestDataToObjectCollectionError(t *testing.T) {
	service, _ := spi.NewSerializationService(serialization.NewConfig())

	_, err := DataToObjectCollection(test.NewNonDeserializableDataSlice(), service)
	assert.Error(t, err)
}

func TestDataToObjectPairCollectionError(t *testing.T) {
	service, _ := spi.NewSerializationService(serialization.NewConfig())

	pairSlice := []*proto.Pair{proto.NewPair(test.NewSerializableData(), test.NewNonDeserializableData())}
	_, err := DataToObjectPairCollection(pairSlice, service)
	assert.Error(t, err)

	pairSlice = []*proto.Pair{proto.NewPair(test.NewNonDeserializableData(), test.NewSerializableData())}
	_, err = DataToObjectPairCollection(pairSlice, service)
	assert.Error(t, err)

}
