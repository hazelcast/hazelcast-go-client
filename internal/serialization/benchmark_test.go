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

package serialization

import (
	"testing"

	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

func BenchmarkService_LookUpDefaultSerializer(b *testing.B) {
	s, err := NewService(&pubserialization.Config{}, nil)
	if err != nil {
		panic(err)
	}
	data, err := s.ToData([]string{"foo1", "foo2", "foo3"})
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		ser := s.LookUpDefaultSerializer(data)
		if ser == nil {
			n++
		}
	}
}
