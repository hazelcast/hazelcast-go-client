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

package cluster

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestMakeRetryPolicy(t *testing.T) {
	config := &cluster.ConnectionRetryConfig{}
	config.Validate()
	r := rand.New(rand.NewSource(1))
	f := makeRetryPolicy(r, config)
	var ts []int64
	for i := 0; i < 10; i++ {
		ts = append(ts, f(i).Milliseconds())
	}
	target := []int64{1000, 1050, 1102, 1157, 1215, 1276, 1340, 1407, 1477, 1551}
	assert.Equal(t, target, ts)
}

func TestMakeRetryPolicy_WithMaxBackoff(t *testing.T) {
	config := &cluster.ConnectionRetryConfig{
		MaxBackoff: types.Duration(1100 * time.Millisecond),
	}
	config.Validate()
	r := rand.New(rand.NewSource(1))
	f := makeRetryPolicy(r, config)
	var ts []int64
	for i := 0; i < 10; i++ {
		ts = append(ts, f(i).Milliseconds())
	}
	target := []int64{1000, 1050, 1100, 1100, 1100, 1100, 1100, 1100, 1100, 1100}
	assert.Equal(t, target, ts)
}

func TestMakeRetryPolicy_WithJitter(t *testing.T) {
	config := &cluster.ConnectionRetryConfig{
		Jitter: 0.2,
	}
	config.Validate()
	r := rand.New(rand.NewSource(1))
	f := makeRetryPolicy(r, config)
	var ts []int64
	for i := 0; i < 10; i++ {
		ts = append(ts, f(i).Milliseconds())
	}
	target := []int64{1041, 1235, 1175, 1128, 1178, 1371, 1107, 1213, 1239, 1427}
	assert.Equal(t, target, ts)
}
