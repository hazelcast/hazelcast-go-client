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

package client

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/stretchr/testify/assert"
)

type person struct {
	Age  int
	Name string
}

func TestHazelcastJsonValue_Nil(t *testing.T) {
	_, err := core.CreateHazelcastJSONValue(nil)
	assert.Error(t, err)
}

func TestHazelcastJsonValue_Chan(t *testing.T) {
	_, err := core.CreateHazelcastJSONValue(make(chan int))
	assert.Error(t, err)
}

func TestHazelcastJsonValue_UnMarshalBack(t *testing.T) {
	expected := person{
		Age: 30, Name: "Name1",
	}

	value, err := core.CreateHazelcastJSONValue(expected)
	assert.NoError(t, err)
	var actual person
	err = value.Unmarshal(&actual)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}
