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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestCompactSerializer(t *testing.T) {
	testCases := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "AddDuplicateField", f: addDuplicateFieldTest},
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
