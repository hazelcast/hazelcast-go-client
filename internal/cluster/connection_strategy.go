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
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func makeRetryPolicy(r *rand.Rand, config *cluster.ConnectionRetryConfig) cb.RetryPolicyFunc {
	backoff := int64(minDuration(config.MaxBackoff, config.InitialBackoff))
	jitter := config.Jitter
	multiplier := config.Multiplier
	return func(attempt int) time.Duration {
		d1 := float64(backoff) + float64(backoff)*jitter*(2.0*r.Float64()-1.0)
		backoff = int64(float64(backoff) * multiplier)
		return time.Duration(d1)
	}
}

func minDuration(d1, d2 types.Duration) types.Duration {
	if d1 < d2 {
		return d1
	}
	return d2
}
