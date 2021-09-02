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

package proto

import (
	"github.com/hazelcast/hazelcast-go-client/types"
)

/*
VectorClock consists of distinct replica logical clocks.

The vector clock may be read from different thread but concurrent
updates must be synchronized externally. There is no guarantee for
concurrent updates.

See Also: https://en.wikipedia.org/wiki/Vector_clock
*/
type VectorClock map[types.UUID]int64

func NewVectorClock() VectorClock {
	return map[types.UUID]int64{}
}

func NewVectorClockFromPairs(pairs []Pair) VectorClock {
	vc := map[types.UUID]int64{}
	for _, p := range pairs {
		vc[p.Key().(types.UUID)] = p.Value().(int64)
	}
	return vc
}

func (vc VectorClock) SetReplicaTimestamp(id types.UUID, ts int64) {
	vc[id] = ts
}

func (vc VectorClock) EntrySet() []Pair {
	entries := make([]Pair, 0, len(vc))
	for id, ts := range vc {
		entries = append(entries, NewPair(id, ts))
	}
	return entries
}

func (vc VectorClock) Size() int {
	return len(vc)
}

// After returns true if this vector clock is causally strictly after the provided vector clock.
// This means that it the provided clock is neither equal to, greater than or concurrent to this vector clock.
func (vc VectorClock) After(other VectorClock) bool {
	for id, ts := range other {
		if localTS, ok := vc[id]; ok {
			if localTS < ts {
				break
			}
			if localTS > ts {
				return true
			}
			continue
		}
		return false
	}
	return len(other) < len(vc)
}
