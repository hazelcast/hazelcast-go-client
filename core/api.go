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

package core

// IAddress represents an address of a member in the cluster.
type IAddress interface {
	// Host returns host of the member.
	Host() string

	// Port returns the port of the member.
	Port() int
}

// IMember represents a member in the cluster with its address, UUID, lite member status and attributes.
type IMember interface {
	// Address returns the address of this member.
	Address() IAddress

	// Uuid returns the UUID of this member.
	Uuid() string

	// IsLiteMember returns true if this member is a lite member.
	IsLiteMember() bool

	// Attributes returns configured attributes for this member.
	Attributes() map[string]string
}

// IPair represents IMap entry pair.
type IPair interface {
	// Key returns key of entry.
	Key() interface{}

	// Values returns value of entry.
	Value() interface{}
}

// IDistributedObjectInfo contains name and service name of distributed objects.
type IDistributedObjectInfo interface {
	// Name returns the name of distributed object.
	Name() string

	// ServiceName returns the service name of distributed object.
	ServiceName() string
}

// IError contains error information that occurred in the server.
type IError interface {
	// ErrorCode returns the error code.
	ErrorCode() int32

	// ClassName returns the class name where error occurred.
	ClassName() string

	// Message returns the error message.
	Message() string

	// StackTrace returns a slice of StackTraceElement.
	StackTrace() []IStackTraceElement

	// CauseErrorCode returns the cause error code.
	CauseErrorCode() int32

	// CauseClassName returns the cause class name.
	CauseClassName() string
}

type IStackTraceElement interface {
	// DeclaringClass returns the fully qualified name of the class containing
	// the execution point represented by the stack trace element.
	DeclaringClass() string

	// MethodName returns the name of the method containing the execution point
	// represented by this stack trace element.
	MethodName() string

	// FileName returns the name of the file containing the execution point
	// represented by the stack trace element, or nil if
	// this information is unavailable.
	FileName() string

	// LineNumber returns the line number of the source line containing the
	// execution point represented by this stack trace element, or
	// a negative number if this information is unavailable. A value
	// of -2 indicates that the method containing the execution point
	// is a native method.
	LineNumber() int32
}

// IEntryView represents a readonly view of a map entry.
type IEntryView interface {
	// Key returns the key of the entry.
	Key() interface{}

	// Value returns the value of the entry.
	Value() interface{}

	// Cost returns the cost in bytes of the entry.
	Cost() int64

	// CreationTime returns the creation time of the entry.
	CreationTime() int64

	// ExpirationTime returns the expiration time of the entry.
	ExpirationTime() int64

	// Hits returns the number of hits of the entry.
	Hits() int64

	// LastAccessTime returns the last access time for the entry.
	LastAccessTime() int64

	// LastStoredTime returns the last store time for the value.
	LastStoredTime() int64

	// LastUpdateTime returns the last time the value was updated.
	LastUpdateTime() int64

	// Version returns the version of the entry.
	Version() int64

	// EvictionCriteriaNumber returns the criteria number for eviction.
	EvictionCriteriaNumber() int64

	// Ttl returns the last set time to live second.
	Ttl() int64
}

type IEntryEvent interface {
	// KeyData returns the key of the entry event.
	Key() interface{}

	// ValueData returns the value of the entry event.
	Value() interface{}

	// OldValueData returns the old value of the entry event.
	OldValue() interface{}

	// MergingValueData returns the incoming merging value of the entry event.
	MergingValue() interface{}

	// EventType returns the type of entry event.
	EventType() int32

	// Uuid returns the Uuid of the member.
	Uuid() *string
}

// IMapEvent is map events common contract.
type IMapEvent interface {
	// EventType returns the event type.
	EventType() int32

	// Uuid returns the Uuid of the member.
	Uuid() *string

	// NumberOfAffectedEntries returns the number of affected
	// entries by this event.
	NumberOfAffectedEntries() int32
}

// IEntryAddedListener is invoked upon addition of an entry.
type IEntryAddedListener interface {
	// EntryAdded is invoked upon addition of an entry.
	EntryAdded(IEntryEvent)
}

// IEntryRemovedListener invoked upon removal of an entry.
type IEntryRemovedListener interface {
	// EntryRemoved invoked upon removal of an entry.
	EntryRemoved(IEntryEvent)
}

// IEntryUpdatedListener is invoked upon update of an entry.
type IEntryUpdatedListener interface {
	// EntryUpdated is invoked upon update of an entry.
	EntryUpdated(IEntryEvent)
}

// IEntryEvictedListener is invoked upon eviction of an entry.
type IEntryEvictedListener interface {
	// EntryEvicted is invoked upon eviction of an entry.
	EntryEvicted(IEntryEvent)
}

// IEntryEvictAllListener is invoked when all entries are evicted
// by IMap.EvictAll() method.
type IEntryEvictAllListener interface {
	// EntryEvictAll is invoked when all entries are evicted
	// by IMap.EvictAll() method.
	EntryEvictAll(IMapEvent)
}

// IEntryClearAllListener is invoked when all entries are removed
// by Imap.Clear() method.
type IEntryClearAllListener interface {
	// EntryClearAll is invoked when all entries are removed
	// by Imap.Clear() method.
	EntryClearAll(IMapEvent)
}

// IEntryMergedListener is invoked after WAN replicated entry is merged.
type IEntryMergedListener interface {
	// EntryMerged is invoked after WAN replicated entry is merged.
	EntryMerged(IEntryEvent)
}

// IEntryExpiredListener which is notified after removal of an entry due to the expiration-based-eviction.
type IEntryExpiredListener interface {
	// EntryExpired is invoked upon expiration of an entry.
	EntryExpired(IEntryEvent)
}

// IMemberAddedListener is invoked when a new member is added to the cluster.
type IMemberAddedListener interface {
	// MemberAdded is invoked when a new member is added to the cluster.
	MemberAdded(member IMember)
}

// IMemberRemovedListener is invoked when an existing member leaves the cluster.
type IMemberRemovedListener interface {
	// MemberRemoved is invoked when an existing member leaves the cluster.
	MemberRemoved(member IMember)
}

// ILifecycleListener is a listener object for listening to lifecycle events of the Hazelcast instance.
type ILifecycleListener interface {
	// LifecycleStateChanged is called when instance's state changes. No blocking calls should be made in this method.
	LifecycleStateChanged(string)
}

// TopicMessageListener is a listener for ITopic.
// Provided that a TopicMessageListener is not registered twice, a TopicMessageListener will never be called concurrently. So there
// is no need to provide thread-safety on internal state in the TopicMessageListener. Also there is no need to enforce safe
// publication, the ITopic is responsible for the memory consistency effects. In other words, there is no need to make
// internal fields of the TopicMessageListener volatile or access them using synchronized blocks.
type TopicMessageListener interface {

	// OnMessage is invoked when a message is received for the added topic. Note that topic guarantees message ordering.
	// Therefore there is only one thread invoking OnMessage.
	OnMessage(message TopicMessage)
}

// TopicMessage is a message for ITopic.
type TopicMessage interface {

	// MessageObject returns the published message.
	MessageObject() interface{}

	// PublishTime returns the time in milliseconds when the message is published.
	PublishTime() int64

	// PublishMember returns the member that published the message.
	// The member can be nil if:
	//    - The message was sent by a client and not a member.
	//    - The member, that sent the message, left the cluster before the message was processed.
	PublishingMember() IMember
}
