// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package core

import . "github.com/hazelcast/go-client/serialization"

type IAddress interface {
	Host() string
	Port() int
}
type IMember interface {
	Address() IAddress
	Uuid() string
	IsLiteMember() bool
	Attributes() map[string]string
}
type IPair interface {
	Key() interface{}
	Value() interface{}
}
type IDistributedObjectInfo interface {
	Name() string
	ServiceName() string
}
type IError interface {
	ErrorCode() int32
	ClassName() string
	Message() string
	StackTrace() []IStackTraceElement
	CauseErrorCode() int32
	CauseClassName() string
}

type IStackTraceElement interface {
	DeclaringClass() string
	MethodName() string
	FileName() string
	LineNumber() int32
}

type IEntryView interface {
	Key() IData
	Value() IData
	Cost() int64
	CreationTime() int64
	ExpirationTime() int64
	Hits() int64
	LastAccessTime() int64
	LastStoredTime() int64
	LastUpdateTime() int64
	Version() int64
	EvictionCriteriaNumber() int64
	Ttl() int64
}
type IEntryEvent interface {
	KeyData() IData
	ValueData() IData
	OldValueData() IData
	MergingValueData() IData
	EventType() int32
	Uuid() *string
}
type IMapEvent interface {
	EventType() int32
	Uuid() *string
	NumberOfAffectedEntries() int32
}
type IEntryAddedListener interface {
	EntryAdded(IEntryEvent)
}
type IEntryRemovedListener interface {
	EntryRemoved(IEntryEvent)
}
type IEntryUpdatedListener interface {
	EntryUpdated(IEntryEvent)
}
type IEntryEvictedListener interface {
	EntryEvicted(IEntryEvent)
}
type IEntryEvictAllListener interface {
	EntryEvictAll(IMapEvent)
}
type IEntryClearAllListener interface {
	EntryClearAll(IMapEvent)
}
type IEntryMergedListener interface {
	EntryMerged(IEntryEvent)
}
type IEntryExpiredListener interface {
	EntryExpired(IEntryEvent)
}
type IMemberAddedListener interface {
	MemberAdded(member IMember)
}
type IMemberRemovedListener interface {
	MemberRemoved(member IMember)
}
type ILifecycleListener interface {
	LifecycleStateChanged(string)
}
