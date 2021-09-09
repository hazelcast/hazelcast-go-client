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
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestTopic_Publish(t *testing.T) {
	it.TopicTester(t, func(t *testing.T, tp *hz.Topic) {
		handlerValue := atomic.Value{}
		handlerValue.Store("base-value")
		subscriptionID, err := tp.AddMessageListener(context.Background(), func(event *hz.MessagePublished) {
			handlerValue.Store("value1")
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = tp.Publish(context.Background(), "HEY!"); err != nil {
			t.Fatal(err)
		}
		it.Eventually(t, func() bool { return assert.Equal(t, "value1", handlerValue.Load()) })

		if err := tp.RemoveListener(context.Background(), subscriptionID); err != nil {
			t.Fatal(err)
		}
		handlerValue.Store("base-value")
		if err = tp.Publish(context.Background(), "HEY!"); err != nil {
			t.Fatal(err)
		}
		it.Never(t, func() bool { return "base-value" != handlerValue.Load() })
	})
}
func TestTopic_PublishAll(t *testing.T) {
	it.TopicTester(t, func(t *testing.T, tp *hz.Topic) {
		handlerValue := int32(0)
		subscriptionID, err := tp.AddMessageListener(context.Background(), func(event *hz.MessagePublished) {
			atomic.AddInt32(&handlerValue, 1)
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = tp.PublishAll(context.Background(), "v1", "v2", "v3"); err != nil {
			t.Fatal(err)
		}
		it.Eventually(t, func() bool { return assert.Equal(t, int32(3), atomic.LoadInt32(&handlerValue)) })

		if err := tp.RemoveListener(context.Background(), subscriptionID); err != nil {
			t.Fatal(err)
		}
		if err = tp.PublishAll(context.Background(), "v4", "v5", "v6"); err != nil {
			t.Fatal(err)
		}
		it.Never(t, func() bool { return int32(3) != atomic.LoadInt32(&handlerValue) })
	})
}
