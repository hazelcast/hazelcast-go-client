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
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"sync"
)

const (
	FId                = 0
	VectorClockClassId = 43
)

type VectorClock struct {
	mutex             sync.RWMutex //guards replicaTimestamps
	replicaTimestamps map[*string]int64
}

func NewVectorClock() *VectorClock {
	return &VectorClock{replicaTimestamps: make(map[*string]int64)}
}

func (*VectorClock) FactoryId() int32 {
	return FId
}

func (*VectorClock) ClassId() int32 {
	return VectorClockClassId
}

func (v *VectorClock) WriteData(output serialization.DataOutput) (err error) {
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	output.WriteInt32(int32(len(v.replicaTimestamps)))
	for key, value := range v.replicaTimestamps {
		output.WriteUTF(*key)
		output.WriteInt64(value)
	}
	return
}

func (v *VectorClock) ReadData(input serialization.DataInput) error {
	stateSize, err := input.ReadInt32()
	if err != nil {
		return err
	}
	for i := int32(0); i < stateSize; i++ {
		replicaId, err := input.ReadUTF()
		if err != nil {
			return err
		}
		timestamp, err := input.ReadInt64()
		if err != nil {
			return err
		}
		v.replicaTimestamps[&replicaId] = timestamp
	}
	return nil
}

func (v *VectorClock) EntrySet() (entrySet []*Pair) {
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	entrySet = make([]*Pair, len(v.replicaTimestamps))
	i := 0
	for key, value := range v.replicaTimestamps {
		entrySet[i] = NewPair(key, value)
		i += 1
	}
	return entrySet
}

func (v *VectorClock) IsAfter(other *VectorClock) (isAfter bool) {
	anyTimestampGreater := false
	for replicaId, otherReplicaTimestamp := range other.replicaTimestamps {
		localReplicaTimestamp, initialized := v.TimestampForReplica(replicaId)
		if !initialized || localReplicaTimestamp < otherReplicaTimestamp {
			return false
		} else if localReplicaTimestamp > otherReplicaTimestamp {
			anyTimestampGreater = true
		}
	}
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	return anyTimestampGreater || (len(v.replicaTimestamps) > len(other.replicaTimestamps))
}

func (v *VectorClock) SetReplicaTimestamp(replicaId *string, timestamp int64) {
	v.mutex.Lock()
	v.replicaTimestamps[replicaId] = timestamp
	v.mutex.Unlock()
}

func (v *VectorClock) TimestampForReplica(replicaId *string) (int64, bool) {
	v.mutex.RLock()
	val, ok := v.replicaTimestamps[replicaId]
	v.mutex.RUnlock()
	return val, ok
}
