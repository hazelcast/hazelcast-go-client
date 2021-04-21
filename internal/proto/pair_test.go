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

package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPair_With_Int(t *testing.T) {
	//given
	key := 10
	value := 100

	//when
	pair := NewPair(key, value)

	//then
	assert.Equal(t, pair.key, key)
	assert.Equal(t, pair.value, value)
}

func TestNewPair_With_String(t *testing.T) {
	//given
	key := "10"
	value := "100"

	//when
	pair := NewPair(key, value)

	//then
	assert.Equal(t, pair.key, key)
	assert.Equal(t, pair.value, value)
}

func TestNewPair_With_ByteArray(t *testing.T) {
	//given
	key := []byte("key")
	value := []byte("value")

	//when
	pair := NewPair(key, value)

	//then
	assert.Equal(t, pair.key, key)
	assert.Equal(t, pair.value, value)
}

func TestNewPair_With_IntArray(t *testing.T) {
	//given
	key := []int32{10}
	value := []int32{100}

	//when
	pair := NewPair(key, value)

	//then
	assert.Equal(t, pair.key, key)
	assert.Equal(t, pair.value, value)
}
