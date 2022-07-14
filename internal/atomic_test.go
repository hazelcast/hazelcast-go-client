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

package internal_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal"
)

func TestAtomicValue_Store(t *testing.T) {
	type St struct {
		a int
		b bool
	}
	testCases := []struct {
		value interface{}
	}{
		{value: nil},
		{value: "foo"},
		{value: 123},
		{value: []string{"foo", "bar", "zoo"}},
		{value: St{a: 10, b: false}},
		{value: &St{a: 10, b: false}},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			av := internal.AtomicValue{}
			av.Store(&tc.value)
			ret := av.Load()
			assert.Equal(t, tc.value, ret)
		})
	}
}
