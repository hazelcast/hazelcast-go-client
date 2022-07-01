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

const (
	TypeFieldOffset                      = 0
	MessageTypeOffset                    = 0
	ByteSizeInBytes                      = 1
	BooleanSizeInBytes                   = 1
	ShortSizeInBytes                     = 2
	CharSizeInBytes                      = 2
	IntSizeInBytes                       = 4
	FloatSizeInBytes                     = 4
	LongSizeInBytes                      = 8
	DoubleSizeInBytes                    = 8
	UUIDSizeInBytes                      = 17
	UuidSizeInBytes                      = 17 // Deprecated
	EntrySizeInBytes                     = UUIDSizeInBytes + LongSizeInBytes
	EntryListIntegerUUIDEntrySizeInBytes = IntSizeInBytes + UUIDSizeInBytes
	LocalDateSizeInBytes                 = IntSizeInBytes + 2*ByteSizeInBytes
	LocalTimeSizeInBytes                 = 3*ByteSizeInBytes + IntSizeInBytes
	LocalDateTimeSizeInBytes             = LocalDateSizeInBytes + LocalTimeSizeInBytes
	OffsetDateTimeSizeInBytes            = LocalDateTimeSizeInBytes + IntSizeInBytes

	CorrelationIDFieldOffset   = TypeFieldOffset + IntSizeInBytes
	CorrelationIDOffset        = MessageTypeOffset + IntSizeInBytes
	FragmentationIDOffset      = 0
	PartitionIDOffset          = CorrelationIDOffset + LongSizeInBytes
	RequestThreadIDOffset      = PartitionIDOffset + IntSizeInBytes
	RequestTTLOffset           = RequestThreadIDOffset + LongSizeInBytes
	RequestIncludeValueOffset  = PartitionIDOffset + IntSizeInBytes
	RequestListenerFlagsOffset = RequestIncludeValueOffset + BooleanSizeInBytes
	RequestLocalOnlyOffset     = RequestListenerFlagsOffset + IntSizeInBytes
	RequestReferenceIdOffset   = RequestTTLOffset + LongSizeInBytes
	ResponseBackupAcksOffset   = CorrelationIDOffset + LongSizeInBytes
	UnfragmentedMessage        = BeginFragmentFlag | EndFragmentFlag

	DefaultFlags              = 0
	BeginFragmentFlag         = 1 << 15
	EndFragmentFlag           = 1 << 14
	IsFinalFlag               = 1 << 13
	BeginDataStructureFlag    = 1 << 12
	EndDataStructureFlag      = 1 << 11
	IsNullFlag                = 1 << 10
	IsEventFlag               = 1 << 9
	BackupEventFlag           = 1 << 7
	SizeOfFrameLengthAndFlags = IntSizeInBytes + ShortSizeInBytes
)
