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

package hazelcast

import (
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
)

// EntryEventType is the type of an entry event.
type EntryEventType int32

const (
	// EntryAdded is dispatched if an entry is added.
	EntryAdded EntryEventType = 1 << 0
	// EntryRemoved is dispatched if an entry is removed.
	EntryRemoved EntryEventType = 1 << 1
	// EntryUpdated is dispatched if an entry is updated.
	EntryUpdated EntryEventType = 1 << 2
	// EntryEvicted is dispatched if an entry is evicted.
	EntryEvicted EntryEventType = 1 << 3
	// EntryExpired is dispatched if an entry is expired.
	EntryExpired EntryEventType = 1 << 4
	// EntryAllEvicted is dispatched if all entries are evicted.
	EntryAllEvicted EntryEventType = 1 << 5
	// EntryAllCleared is dispatched if all entries are cleared.
	EntryAllCleared EntryEventType = 1 << 6
	// EntryMerged is dispatched if an entry is merged after a network partition.
	EntryMerged EntryEventType = 1 << 7
	// EntryInvalidated is dispatched if an entry is invalidated.
	EntryInvalidated EntryEventType = 1 << 8
	// EntryLoaded is dispatched if an entry is loaded.
	EntryLoaded EntryEventType = 1 << 9
)

// EntryNotifiedHandler is called when an entry event happens.
type EntryNotifiedHandler func(event *EntryNotified)

const (
	eventEntryNotified              = "entrynotified"
	eventLifecycleEventStateChanged = "lifecyclestatechanged"
	eventMessagePublished           = "messagepublished"
	eventQueueItemNotified          = "queue.itemnotified"
	eventListItemNotified           = "list.itemnotified"
	eventSetItemNotified            = "set.itemnotified"
	eventDistributedObjectNotified  = "distributedobjectnotified"
)

// EntryNotified contains information about an entry event.
type EntryNotified struct {
	MergingValue            interface{}
	Key                     interface{}
	Value                   interface{}
	OldValue                interface{}
	MapName                 string
	Member                  cluster.MemberInfo
	NumberOfAffectedEntries int
	EventType               EntryEventType
}

func (e *EntryNotified) EventName() string {
	return eventEntryNotified
}

func newEntryNotifiedEvent(
	mapName string,
	member cluster.MemberInfo,
	key interface{},
	value interface{},
	oldValue interface{},
	mergingValue interface{},
	numberOfAffectedEntries int,
	eventType EntryEventType,
) *EntryNotified {
	return &EntryNotified{
		MapName:                 mapName,
		Member:                  member,
		Key:                     key,
		Value:                   value,
		OldValue:                oldValue,
		MergingValue:            mergingValue,
		NumberOfAffectedEntries: numberOfAffectedEntries,
		EventType:               eventType,
	}
}

// LifecycleState indicates the state of the lifecycle event.
type LifecycleState int

func (s LifecycleState) String() string {
	switch s {
	case LifecycleStateStarting:
		return "starting"
	case LifecycleStateStarted:
		return "started"
	case LifecycleStateShuttingDown:
		return "shutting down"
	case LifecycleStateShutDown:
		return "shutdown"
	case LifecycleStateConnected:
		return "client connected"
	case LifecycleStateDisconnected:
		return "client disconnected"
	default:
		return "UNKNOWN"
	}
}

const (
	// LifecycleStateStarting signals that the client is starting.
	LifecycleStateStarting LifecycleState = iota
	// LifecycleStateStarted signals that the client started.
	LifecycleStateStarted
	// LifecycleStateShuttingDown signals that the client is shutting down.
	LifecycleStateShuttingDown
	// LifecycleStateShutDown signals that the client shut down.
	LifecycleStateShutDown
	// LifecycleStateConnected signals that the client connected to the cluster.
	LifecycleStateConnected
	// LifecycleStateDisconnected signals that the client disconnected from the cluster.
	LifecycleStateDisconnected
)

// LifecycleStateChangeHandler is called when a lifecycle event occurs.
type LifecycleStateChangeHandler func(event LifecycleStateChanged)

// LifecycleStateChanged contains information about a lifecycle event.
type LifecycleStateChanged struct {
	State LifecycleState
}

func (e *LifecycleStateChanged) EventName() string {
	return eventLifecycleEventStateChanged
}

func newLifecycleStateChanged(state LifecycleState) *LifecycleStateChanged {
	return &LifecycleStateChanged{State: state}
}

// MessagePublished contains information about a message published event.
type MessagePublished struct {
	PublishTime time.Time
	Value       interface{}
	TopicName   string
	Member      cluster.MemberInfo
}

func (m *MessagePublished) EventName() string {
	return eventMessagePublished
}

func newMessagePublished(name string, value interface{}, publishTime time.Time, member cluster.MemberInfo) *MessagePublished {
	return &MessagePublished{
		TopicName:   name,
		Value:       value,
		PublishTime: publishTime,
		Member:      member,
	}
}

// ItemEventType describes event types for item related events.
type ItemEventType int32

const (
	// ItemAdded stands for item added event.
	ItemAdded ItemEventType = 1
	// ItemRemoved stands for item removed event.
	ItemRemoved ItemEventType = 2
)

// QueueItemNotifiedHandler is called when an item notified event is generated for a Queue.
type QueueItemNotifiedHandler func(event *QueueItemNotified)

// QueueItemNotified contains information about an item notified event.
type QueueItemNotified struct {
	Value     interface{}
	QueueName string
	Member    cluster.MemberInfo
	EventType ItemEventType
}

func (q QueueItemNotified) EventName() string {
	return eventQueueItemNotified
}

func newQueueItemNotified(name string, value interface{}, member cluster.MemberInfo, eventType int32) *QueueItemNotified {
	return &QueueItemNotified{
		QueueName: name,
		Value:     value,
		Member:    member,
		EventType: ItemEventType(eventType),
	}
}

// ListItemNotifiedHandler is a handler function for the List item listener.
type ListItemNotifiedHandler func(event *ListItemNotified)

// ListItemNotified describes the List item event.
type ListItemNotified struct {
	Value     interface{}
	ListName  string
	Member    cluster.MemberInfo
	EventType ItemEventType
}

// EventName returns generic event name, common for all List item listeners.
func (q ListItemNotified) EventName() string {
	return eventListItemNotified
}

func newListItemNotified(name string, value interface{}, member cluster.MemberInfo, eventType int32) *ListItemNotified {
	return &ListItemNotified{
		ListName:  name,
		Value:     value,
		Member:    member,
		EventType: ItemEventType(eventType),
	}
}

// SetItemNotifiedHandler is called when an item notified event is generated for a Set.
type SetItemNotifiedHandler func(event *SetItemNotified)

// SetItemNotified contains information about an item notified event.
type SetItemNotified struct {
	Value     interface{}
	SetName   string
	Member    cluster.MemberInfo
	EventType ItemEventType
}

func (q SetItemNotified) EventName() string {
	return eventSetItemNotified
}

func newSetItemNotified(name string, value interface{}, member cluster.MemberInfo, eventType int32) *SetItemNotified {
	return &SetItemNotified{
		SetName:   name,
		Value:     value,
		Member:    member,
		EventType: ItemEventType(eventType),
	}
}

// DistributedObjectEventType describes event type of a distributed object.
type DistributedObjectEventType string

const (
	// DistributedObjectCreated is the event type when a distributed object is created.
	DistributedObjectCreated DistributedObjectEventType = "CREATED"
	// DistributedObjectDestroyed is the event type when a distributed object is destroyed.
	DistributedObjectDestroyed DistributedObjectEventType = "DESTROYED"
)

// DistributedObjectNotifiedHandler is called when a distribute object event occurs.
type DistributedObjectNotifiedHandler func(event DistributedObjectNotified)

// DistributedObjectNotified contains informatino about the distributed object event.
type DistributedObjectNotified struct {
	ServiceName string
	ObjectName  string
	EventType   DistributedObjectEventType
}

func (d DistributedObjectNotified) EventName() string {
	return eventDistributedObjectNotified
}

func newDistributedObjectNotified(service string, object string, eventType DistributedObjectEventType) DistributedObjectNotified {
	return DistributedObjectNotified{
		ServiceName: service,
		ObjectName:  object,
		EventType:   eventType,
	}
}
