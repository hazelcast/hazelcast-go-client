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

package invalidation

import "sync/atomic"

type MetaDataContainer struct {
	sequence            int64
	staleSequence       int64
	missedSequenceCount int64
	uuid                atomic.Value
}

func NewMetaDataContainer() *MetaDataContainer {
	m := &MetaDataContainer{}
	m.uuid.Store("")
	return m
}

func (m *MetaDataContainer) UUID() string {
	return m.uuid.Load().(string)
}

func (m *MetaDataContainer) SetUUID(uuid string) {
	m.uuid.Store(uuid)
}

func (m *MetaDataContainer) CompareAndSetUUID(prevUUID string, newUUID string) bool {
	if currentUUID := m.uuid.Load().(string); currentUUID == prevUUID {
		m.uuid.Store(newUUID)
		return true
	}
	return false
}

func (m *MetaDataContainer) SetSequence(sequence int64) {
	atomic.StoreInt64(&m.sequence, sequence)
}

func (m *MetaDataContainer) Sequence() int64 {
	return atomic.LoadInt64(&m.sequence)
}

func (m *MetaDataContainer) CompareAndSetSequence(prevSequence int64, newSequence int64) bool {
	if currentSeq := atomic.LoadInt64(&m.sequence); currentSeq == prevSequence {
		atomic.StoreInt64(&m.sequence, newSequence)
		return true
	}
	return false
}

func (m *MetaDataContainer) ResetSequence() {
	atomic.StoreInt64(&m.sequence, 0)
}

func (m *MetaDataContainer) StaleSequence() int64 {
	return atomic.LoadInt64(&m.staleSequence)
}

func (m *MetaDataContainer) CompareAndSetStaleSequence(lastKnownStaleSeq int64, lastReceivedSeq int64) bool {
	if currentStaleSeq := atomic.LoadInt64(&m.staleSequence); currentStaleSeq == lastKnownStaleSeq {
		atomic.StoreInt64(&m.staleSequence, lastReceivedSeq)
		return true
	}
	return false
}

func (m *MetaDataContainer) ResetStaleSequence() {
	atomic.StoreInt64(&m.staleSequence, 0)
}

func (m *MetaDataContainer) AddAndGetMissedSequenceCount(missCount int64) int64 {
	return atomic.AddInt64(&m.missedSequenceCount, missCount)
}

func (m *MetaDataContainer) MissedSequenceCount() int64 {
	return atomic.LoadInt64(&m.missedSequenceCount)
}
