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

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/it"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestNullableInteroperability(t *testing.T) {
	testCases := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "WriteNullableReadPrimitive", f: writeNullableReadPrimitiveTest},
		{name: "WriteNullReadPrimitiveThrowsException", f: writeNullReadPrimitiveThrowsExceptionTest},
		{name: "WritePrimitiveReadNullable", f: writePrimitiveReadNullableTest},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.f(t)
		})
	}
}

func writePrimitiveReadNullableTest(t *testing.T) {
	test := NewCompactTestObj()
	config := serialization.Config{}
	config.Compact.SetSerializers(CompactTestWritePrimitiveReadNullableSerializer{})
	ss := mustSerializationService(iserialization.NewService(&config, nil))
	data := it.MustValue(ss.ToData(test)).(iserialization.Data)
	obj := it.MustValue(ss.ToObject(data))
	if !reflect.DeepEqual(test, obj) {
		t.Fatalf("%+v, %+v are expected to be equal but they are not", test, obj)
	}
}

func writeNullableReadPrimitiveTest(t *testing.T) {
	test := NewCompactTestObj()
	config := serialization.Config{}
	config.Compact.SetSerializers(CompactTestWriteNullableReadPrimitiveSerializer{})
	ss := mustSerializationService(iserialization.NewService(&config, nil))
	data := it.MustValue(ss.ToData(test)).(iserialization.Data)
	obj := it.MustValue(ss.ToObject(data))
	if !reflect.DeepEqual(test, obj) {
		t.Fatalf("%+v, %+v are expected to be equal but they are not", test, obj)
	}
}

func writeNullReadPrimitiveThrowsExceptionTest(t *testing.T) {
	test := NewCompactTestObj()
	config := serialization.Config{}
	config.Compact.SetSerializers(CompactTestWriteNullReadPrimitiveSerializer{})
	ss := mustSerializationService(iserialization.NewService(&config, nil))
	data := it.MustValue(ss.ToData(test)).(iserialization.Data)
	_, err := ss.ToObject(data)
	// TODO: think how to test all Read() methods, not just boolean. When generic records implemented change this test to do that.
	assert.NotNilf(t, err, "Expected the error to be not nil but it is nil")
	assert.Containsf(t, err.Error(), " nil value cannot be read via read", "expected error to contain ' nil value cannot be read via read' but it was %s", err.Error())
}
