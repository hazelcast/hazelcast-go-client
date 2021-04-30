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

package hazelcast_test

import (
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestQueueOfferTake(t *testing.T) {
	it.QueueTest(t, func(t *testing.T, q *hz.Queue) {
		targetValue := "item1"
		if ok, err := q.Offer(targetValue); err != nil {
			t.Fatal(err)
		} else {
			assert.True(t, ok)
		}
		if value, err := q.Take(); err != nil {
			assert.Equal(t, targetValue, value)
		}
	})
}
