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

package it

import (
	"context"

	"github.com/hazelcast/hazelcast-go-client"
)

type DataStructureAdapter interface {
	Get(ctx context.Context, key interface{}) (interface{}, error)
	Put(ctx context.Context, key interface{}, value interface{}) (interface{}, error)
	LocalMapStats() hazelcast.LocalMapStats
}

type MapDataStructureAdapter struct {
	m *hazelcast.Map
}

func NewMapDataStructureAdapter(m *hazelcast.Map) *MapDataStructureAdapter {
	return &MapDataStructureAdapter{m: m}
}

func (m *MapDataStructureAdapter) Get(ctx context.Context, key interface{}) (interface{}, error) {
	return m.m.Get(ctx, key)
}

func (m *MapDataStructureAdapter) Put(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	return m.m.Put(ctx, key, value)
}

func (m *MapDataStructureAdapter) LocalMapStats() hazelcast.LocalMapStats {
	return m.m.LocalMapStats()
}

type NearCacheAdapter interface {
	Size() int
	Get(key interface{}) (interface{}, bool, error)
}
