/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package serialization_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestCompactSerializer(t *testing.T) {
	testCases := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "AddDuplicateField", f: addDuplicateFieldTest},
		{name: "SliceWithDifferentTypes", f: sliceWithDifferentTypesTest},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, tc.f)
	}
}

func addDuplicateFieldTest(t *testing.T) {
	// ported from: com.hazelcast.internal.serialization.impl.compact.CompactSerializationTest#testSerializer_withDuplicateFieldNames
	var cfg pubserialization.Config
	cfg.Compact.SetSerializers(&duplicateWritingSerializer{})
	_, err := serialization.NewService(&cfg, nil)
	require.Error(t, err)
}

func sliceWithDifferentTypesTest(t *testing.T) {
	// ported from: testWritingArrayOfCompactGenericField_withDifferentItemTypes
	var cfg pubserialization.Config
	cfg.Compact.SetSerializers(&sliceWithDifferentTypesSerializer{}, &BitsDTOSerializer{}, &InnerDTOSerializer{})
	ss, err := serialization.NewService(&cfg, nil)
	require.NoError(t, err)
	_, err = ss.ToData(sliceWithDifferentTypes([]interface{}{BitsDTO{}, InnerDTO{}}))
	require.True(t, errors.Is(err, hzerrors.ErrHazelcastSerialization))
}

type duplicatedType struct{}

type duplicateWritingSerializer struct{}

func (d duplicateWritingSerializer) Type() reflect.Type {
	return reflect.TypeOf(duplicatedType{})
}

func (d duplicateWritingSerializer) TypeName() string {
	return "duplicated"
}

func (d duplicateWritingSerializer) Read(reader pubserialization.CompactReader) interface{} {
	return duplicatedType{}
}

func (d duplicateWritingSerializer) Write(writer pubserialization.CompactWriter, value interface{}) {
	writer.WriteInt32("bar", 1)
	writer.WriteInt32("bar", 1)
}

type sliceWithDifferentTypes []interface{}

type sliceWithDifferentTypesSerializer struct{}

func (s sliceWithDifferentTypesSerializer) Type() reflect.Type {
	return reflect.TypeOf(sliceWithDifferentTypes{})
}

func (s sliceWithDifferentTypesSerializer) TypeName() string {
	return "sliceWithDifferentTypes"
}

func (s sliceWithDifferentTypesSerializer) Read(reader pubserialization.CompactReader) interface{} {
	panic("not implemented")
}

func (s sliceWithDifferentTypesSerializer) Write(writer pubserialization.CompactWriter, value interface{}) {
	v := []interface{}(value.(sliceWithDifferentTypes))
	writer.WriteArrayOfCompact("f", v)
}
