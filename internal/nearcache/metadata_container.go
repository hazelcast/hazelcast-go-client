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

type MetaDataContainer interface {
	UUID() string
	SetUUID(uuid string)
	CompareAndSetUUID(prevUUID string, newUUID string) bool
	SetSequence(sequence int64)
	Sequence() int64
	CompareAndSetSequence(prevSequence int64, newSequence int64) bool
	ResetSequence()
	StaleSequence() int64
	CompareAndSetStaleSequence(lastKnownStaleSeq int64, lastReceivedSeq int64) bool
	ResetStaleSequence()
	AddAndGetMissedSequenceCount(missCount int64) int64
	MissedSequenceCount() int64
}
