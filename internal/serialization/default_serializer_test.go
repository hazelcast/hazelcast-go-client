/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package serialization

import (
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/types"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestDefaultSerializer(t *testing.T) {
	testCases := []struct {
		Value  interface{}
		Target interface{}
	}{
		{Value: int8(-42), Target: uint8(0xd6)},
		{Value: types.NewUUIDWith(uint64(1234), uint64(5678)), Target: types.NewUUIDWith(uint64(1234), uint64(5678))},
	}
	sc := &serialization.Config{}
	sc.SetGlobalSerializer(&PanicingGlobalSerializer{})
	service, err := NewService(sc, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range testCases {
		t.Run(reflect.TypeOf(tc.Value).String(), func(t *testing.T) {
			data, err := service.ToData(tc.Value)
			if err != nil {
				t.Fatal(err)
			}
			obj, err := service.ToObject(data)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.Target, obj)
		})
	}
}

type PanicingGlobalSerializer struct{}

func (p PanicingGlobalSerializer) ID() (id int32) {
	return 1000
}

func (p PanicingGlobalSerializer) Read(input serialization.DataInput) interface{} {
	panic("panicing global serializer: read")
}

func (p PanicingGlobalSerializer) Write(output serialization.DataOutput, object interface{}) {
	panic("panicing global serializer: write")
}
