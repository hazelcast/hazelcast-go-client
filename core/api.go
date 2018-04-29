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

// Package core provides core API interfaces/classes.
package core

import "time"

// Address represents an address of a member in the cluster.
type Address interface {
	// Host returns host of the member.
	Host() string

	// Port returns the port of the member.
	Port() int
}

// Member represents a member in the cluster with its address, uuid, lite member status and attributes.
type Member interface {
	// Address returns the address of this member.
	Address() Address

	// UUID returns the uuid of this member.
	UUID() string

	// IsLiteMember returns true if this member is a lite member.
	IsLiteMember() bool

	// Attributes returns configured attributes for this member.
	Attributes() map[string]string
}

// Pair represents Map entry pair.
type Pair interface {
	// Key returns key of entry.
	Key() interface{}

	// Values returns value of entry.
	Value() interface{}
}

// DistributedObjectInfo contains name and service name of distributed objects.
type DistributedObjectInfo interface {
	// Name returns the name of distributed object.
	Name() string

	// ServiceName returns the service name of distributed object.
	ServiceName() string
}

// Error contains error information that occurred in the server.
type Error interface {
	// ErrorCode returns the error code.
	ErrorCode() int32

	// ClassName returns the class name where error occurred.
	ClassName() string

	// Message returns the error message.
	Message() string

	// StackTrace returns a slice of StackTraceElement.
	StackTrace() []StackTraceElement

	// CauseErrorCode returns the cause error code.
	CauseErrorCode() int32

	// CauseClassName returns the cause class name.
	CauseClassName() string
}

type StackTraceElement interface {
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

// EntryView represents a readonly view of a map entry.
type EntryView interface {
	// Key returns the key of the entry.
	Key() interface{}

	// Value returns the value of the entry.
	Value() interface{}

	// Cost returns the cost in bytes of the entry.
	Cost() int64

	// CreationTime returns the creation time of the entry.
	CreationTime() time.Time

	// ExpirationTime returns the expiration time of the entry.
	ExpirationTime() time.Time

	// Hits returns the number of hits of the entry.
	Hits() int64

	// LastAccessTime returns the last access time for the entry.
	LastAccessTime() time.Time

	// LastStoredTime returns the last store time for the value.
	LastStoredTime() time.Time

	// LastUpdateTime returns the last time the value was updated.
	LastUpdateTime() time.Time

	// Version returns the version of the entry.
	Version() int64

	// EvictionCriteriaNumber returns the criteria number for eviction.
	EvictionCriteriaNumber() int64

	// TTL returns the last set time to live second.
	TTL() time.Duration
}

// EntryEvent is map entry event.
type EntryEvent interface {
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

	// UUID returns the uuid of the member.
	UUID() *string
}

// ItemEvent is List, Set and Queue events common contract.
type ItemEvent interface {
	// Name returns the name of List, Set or Queue
	Name() string

	// Item returns the item of the event.
	Item() interface{}

	// EventType returns 1 if an item is added, 2 if an item is removed.
	EventType() int32

	// Member is the member that sent the event.
	Member() Member
}

// MapEvent is map events common contract.
type MapEvent interface {
	// EventType returns the event type.
	EventType() int32

	// UUID returns the uuid of the member.
	UUID() *string

	// NumberOfAffectedEntries returns the number of affected
	// entries by this event.
	NumberOfAffectedEntries() int32
}

// EntryAddedListener is invoked upon addition of an entry.
type EntryAddedListener interface {
	// EntryAdded is invoked upon addition of an entry.
	EntryAdded(EntryEvent)
}

// EntryRemovedListener invoked upon removal of an entry.
type EntryRemovedListener interface {
	// EntryRemoved invoked upon removal of an entry.
	EntryRemoved(EntryEvent)
}

// EntryUpdatedListener is invoked upon update of an entry.
type EntryUpdatedListener interface {
	// EntryUpdated is invoked upon update of an entry.
	EntryUpdated(EntryEvent)
}

// EntryEvictedListener is invoked upon eviction of an entry.
type EntryEvictedListener interface {
	// EntryEvicted is invoked upon eviction of an entry.
	EntryEvicted(EntryEvent)
}

// EntryEvictAllListener is invoked when all entries are evicted
// by Map.EvictAll method.
type EntryEvictAllListener interface {
	// EntryEvictAll is invoked when all entries are evicted
	// by Map.EvictAll method.
	EntryEvictAll(MapEvent)
}

// EntryClearAllListener is invoked when all entries are removed
// by Map.Clear method.
type EntryClearAllListener interface {
	// EntryClearAll is invoked when all entries are removed
	// by Map.Clear method.
	EntryClearAll(MapEvent)
}

// EntryMergedListener is invoked after WAN replicated entry is merged.
type EntryMergedListener interface {
	// EntryMerged is invoked after WAN replicated entry is merged.
	EntryMerged(EntryEvent)
}

// EntryExpiredListener which is notified after removal of an entry due to the expiration-based-eviction.
type EntryExpiredListener interface {
	// EntryExpired is invoked upon expiration of an entry.
	EntryExpired(EntryEvent)
}

// MemberAddedListener is invoked when a new member is added to the cluster.
type MemberAddedListener interface {
	// MemberAdded is invoked when a new member is added to the cluster.
	MemberAdded(member Member)
}

// MemberRemovedListener is invoked when an existing member leaves the cluster.
type MemberRemovedListener interface {
	// MemberRemoved is invoked when an existing member leaves the cluster.
	MemberRemoved(member Member)
}

// LifecycleListener is a listener object for listening to lifecycle events of the Hazelcast instance.
type LifecycleListener interface {
	// LifecycleStateChanged is called when instance's state changes. No blocking calls should be made in this method.
	LifecycleStateChanged(string)
}

// TopicMessageListener is a listener for Topic.
// Provided that a TopicMessageListener is not registered twice, a TopicMessageListener will never be called concurrently.
// So there is no need to provide thread-safety on internal state in the TopicMessageListener. Also there is no need to enforce
// safe publication, the Topic is responsible for the memory consistency effects. In other words, there is no need to make
// internal fields of the TopicMessageListener volatile or access them using synchronized blocks.
type TopicMessageListener interface {
	// OnMessage is invoked when a message is received for the added topic. Note that topic guarantees message ordering.
	// Therefore there is only one thread invoking OnMessage.
	OnMessage(message TopicMessage)
}

// TopicMessage is a message for Topic.
type TopicMessage interface {
	// MessageObject returns the published message.
	MessageObject() interface{}

	// PublishTime returns the time in milliseconds when the message is published.
	PublishTime() time.Time

	// PublishMember returns the member that published the message.
	// The member can be nil if:
	//    - The message was sent by a client and not a member.
	//    - The member, that sent the message, left the cluster before the message was processed.
	PublishingMember() Member
}

// ItemAddedListener is invoked when an item is added.
type ItemAddedListener interface {
	ItemAdded(event ItemEvent)
}

// ItemRemovedListener is invoked when an item is removed.
type ItemRemovedListener interface {
	ItemRemoved(event ItemEvent)
}
