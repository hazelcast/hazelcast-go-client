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

package colutil_test

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization/spi"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/test/testutil"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/colutil"
	"github.com/stretchr/testify/assert"
)

func TestColUtilsNilArgument(t *testing.T) {
	_, err := colutil.ObjectToDataCollection(nil, nil)
	assert.Error(t, err)

	_, err = colutil.DataToObjectCollection(nil, nil)
	assert.Error(t, err)
}

func TestObjectToDataCollectionNonSerializableKey(t *testing.T) {
	service, _ := spi.NewSerializationService(serialization.NewConfig())

	_, err := colutil.ObjectToDataCollection(testutil.NewNonSerializableObjectSlice(), service)
	assert.Error(t, err)
}

func TestDataToObjectCollectionError(t *testing.T) {
	service, _ := spi.NewSerializationService(serialization.NewConfig())

	_, err := colutil.DataToObjectCollection(testutil.NewNonDeserializableDataSlice(), service)
	assert.Error(t, err)
}

func TestDataToObjectPairCollectionError(t *testing.T) {
	service, _ := spi.NewSerializationService(serialization.NewConfig())

	pair1 := proto.NewPair(testutil.NewSerializableData(), testutil.NewNonDeserializableData())
	pairSlice := []*proto.Pair{&pair1}
	_, err := colutil.DataToObjectPairCollection(pairSlice, service)
	assert.Error(t, err)

	pair2 := proto.NewPair(testutil.NewNonDeserializableData(), testutil.NewSerializableData())
	pairSlice = []*proto.Pair{&pair2}
	_, err = colutil.DataToObjectPairCollection(pairSlice, service)
	assert.Error(t, err)

}
