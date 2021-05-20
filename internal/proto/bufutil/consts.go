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

package bufutil

/*
Event Response Constants
*/
const (
	EventMember                 = 200
	EventMemberList             = 201
	EventMemberAttributeChange  = 202
	EventEntry                  = 203
	EventItem                   = 204
	EventTopic                  = 205
	EventPartitionLost          = 206
	EventDistributedObject      = 207
	EventCacheInvalidation      = 208
	EventMapPartitionLost       = 209
	EventCache                  = 210
	EventCacheBatchInvalidation = 211
	// ENTERPRISE
	EventQueryCacheSingle = 212
	EventQueryCacheBatch  = 213

	EventCachePartitionLost    = 214
	EventIMapInvalidation      = 215
	EventIMapBatchInvalidation = 216
)

const (
	MessageTypeEventMembersView = 770
)

const (
	ByteSizeInBytes    = 1
	BoolSizeInBytes    = 1
	Uint8SizeInBytes   = 1
	Int16SizeInBytes   = 2
	Uint16SizeInBytes  = 2
	Int32SizeInBytes   = 4
	Float32SizeInBytes = 4
	Float64SizeInBytes = 8
	Int64SizeInBytes   = 8

	Version            = 0
	BeginFlag    uint8 = 0x80
	EndFlag      uint8 = 0x40
	BeginEndFlag uint8 = BeginFlag | EndFlag
	ListenerFlag uint8 = 0x01

	PayloadOffset = 18
	SizeOffset    = 0

	FrameLengthFieldOffset   = 0
	VersionFieldOffset       = FrameLengthFieldOffset + Int32SizeInBytes
	FlagsFieldOffset         = VersionFieldOffset + ByteSizeInBytes
	TypeFieldOffset          = FlagsFieldOffset + ByteSizeInBytes
	CorrelationIDFieldOffset = TypeFieldOffset + Int16SizeInBytes
	PartitionIDFieldOffset   = CorrelationIDFieldOffset + Int64SizeInBytes
	DataOffsetFieldOffset    = PartitionIDFieldOffset + Int32SizeInBytes
	HeaderSize               = DataOffsetFieldOffset + Int16SizeInBytes

	NilArrayLength = -1
)

const (
	NilKeyIsNotAllowed        string = "nil key is not allowed"
	NilKeysAreNotAllowed      string = "nil keys collection is not allowed"
	NilValueIsNotAllowed      string = "nil value is not allowed"
	NilPredicateIsNotAllowed  string = "predicate should not be nil"
	NilMapIsNotAllowed        string = "nil map is not allowed"
	NilArgIsNotAllowed        string = "nil arg is not allowed"
	NilSliceIsNotAllowed      string = "nil slice is not allowed"
	NilListenerIsNotAllowed   string = "nil listener is not allowed"
	NilAggregatorIsNotAllowed string = "aggregator should not be nil"
	NilProjectionIsNotAllowed string = "projection should not be nil"
)
