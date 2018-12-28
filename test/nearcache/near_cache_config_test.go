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

package nearcache

import (
	"strconv"
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	config2 "github.com/hazelcast/hazelcast-go-client/config"
	"github.com/stretchr/testify/assert"
)

func TestObjectMemoryFormat(t *testing.T) {
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetInMemoryFormat(config2.InMemoryFormatObject)
	client, err := hazelcast.NewClientWithConfig(config)
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		mp.Put(strconv.Itoa(i), int32(i))
		value, err := mp.Get(strconv.Itoa(i))
		assert.NoError(t, err)
		assert.Equal(t, value, int32(i))
		value, err = mp.Get(strconv.Itoa(i))
		assert.NoError(t, err)
		assert.Equal(t, value, int32(i))
	}

	client.Shutdown()
}
