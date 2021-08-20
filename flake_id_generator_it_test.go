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
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/stretchr/testify/assert"
)

func TestFlakeIDGenerator_NewId(t *testing.T) {
	it.Tester(t, func(t *testing.T, client *hz.Client) {
		const idCount = 1_000
		ctx := context.Background()
		f, err := client.GetFlakeIDGenerator(ctx, it.NewUniqueObjectName("flake-id-gen"))
		if err != nil {
			t.Fatal(err)
		}
		ids := map[int64]struct{}{}
		for i := 0; i < idCount; i++ {
			if id, err := f.NewId(ctx); err != nil {
				t.Fatal(err)
			} else {
				ids[id] = struct{}{}
			}
		}
		assert.Equal(t, idCount, len(ids)) // assert uniqueness
	})
}
