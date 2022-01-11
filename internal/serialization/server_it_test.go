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

package serialization_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestServer(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		target interface{}
	}{
		{
			name:   "array",
			input:  `Java.to(["foo", 38], "java.lang.Object[]")`,
			target: []interface{}{"foo", int32(38)},
		},
		{
			name:   "linked-list",
			input:  `new java.util.LinkedList(java.util.Arrays.asList("foo", 38))`,
			target: []interface{}{"foo", int32(38)},
		},
		{
			name:   "class",
			input:  `java.lang.String.class`,
			target: "java.lang.String",
		},
	}
	it.SerializationTester(t, func(t *testing.T, config hazelcast.Config, clusterID, mapName string) {
		ctx := context.Background()
		client := it.MustClient(hazelcast.StartNewClientWithConfig(ctx, config))
		m := it.MustValue(client.GetMap(ctx, mapName)).(*hazelcast.Map)
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				key := fmt.Sprintf(`"%s"`, tc.name)
				res := it.MapSetOnServer(clusterID, mapName, key, tc.input)
				if !res.GetSuccess() {
					t.Fatalf("could not set: %s, %s", key, res.GetMessage())
				}
				v := it.MustValue(m.Get(ctx, tc.name))
				assert.Equal(t, tc.target, v)
			})
		}
	})
}
