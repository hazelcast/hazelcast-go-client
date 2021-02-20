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

package internal

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/stretchr/testify/assert"
)

func TestDefaultNearCacheManager_ListAllNearCaches(t *testing.T) {
	manager := NewDefaultNearCacheManager(nil, nil)
	cfg := config.NewNearCacheConfig()
	manager.GetOrCreateNearCache("nearcache1", cfg)
	manager.GetOrCreateNearCache("nearcache1", cfg)
	manager.GetOrCreateNearCache("nearcache3", cfg)
	allCaches := manager.ListAllNearCaches()
	assert.Len(t, allCaches, 2)
}

func TestDefaultNearCacheManager_DestroyAllNearCaches(t *testing.T) {
	manager := NewDefaultNearCacheManager(nil, nil)
	cfg := config.NewNearCacheConfig()
	manager.GetOrCreateNearCache("nearcache1", cfg)
	manager.GetOrCreateNearCache("nearcache1", cfg)
	manager.GetOrCreateNearCache("nearcache3", cfg)
	manager.DestroyAllNearCaches()
	allCaches := manager.ListAllNearCaches()
	assert.Len(t, allCaches, 0)
}

func TestDefaultNearCacheManager_DestroyNearCache(t *testing.T) {
	manager := NewDefaultNearCacheManager(nil, nil)
	cfg := config.NewNearCacheConfig()
	manager.GetOrCreateNearCache("nearcache1", cfg)
	manager.GetOrCreateNearCache("nearcache1", cfg)
	manager.GetOrCreateNearCache("nearcache3", cfg)
	manager.DestroyNearCache("nearcache1")
	allCaches := manager.ListAllNearCaches()
	assert.Len(t, allCaches, 1)
}

func TestDefaultNearCacheManager_NearCache(t *testing.T) {
	manager := NewDefaultNearCacheManager(nil, nil)
	cfg := config.NewNearCacheConfig()
	manager.GetOrCreateNearCache("nearcache1", cfg)
	_, found := manager.NearCache("nearcache1")
	assert.True(t, found)
	_, found = manager.NearCache("nearcache2")
	assert.False(t, found)
}
