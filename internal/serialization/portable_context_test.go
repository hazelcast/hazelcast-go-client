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
	"sync"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestPortableContext_RegisterClassDefinition_Race(t *testing.T) {
	pc := serialization.NewPortableContext(nil, 0)
	const c = 10_000
	wg := &sync.WaitGroup{}
	wg.Add(c)
	for i := 0; i < c; i++ {
		go func() {
			cd := pubserialization.NewClassDefinition(1, 1, 0)
			it.Must(pc.RegisterClassDefinition(cd))
			wg.Done()
		}()
	}
	wg.Wait()
}
