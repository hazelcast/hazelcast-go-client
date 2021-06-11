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

const (
	// NotifyEntryAdded is dispatched if an entry is added.
	NotifyEntryAdded = int32(1 << 0)
	// NotifyEntryRemoved is dispatched if an entry is removed.
	NotifyEntryRemoved = int32(1 << 1)
	// NotifyEntryUpdated is dispatched if an entry is updated.
	NotifyEntryUpdated = int32(1 << 2)
	// NotifyEntryEvicted is dispatched if an entry is evicted.
	NotifyEntryEvicted = int32(1 << 3)
	// NotifyEntryExpired is dispatched if an entry is expired.
	NotifyEntryExpired = int32(1 << 4)
	// NotifyEntryAllEvicted is dispatched if all entries are evicted.
	NotifyEntryAllEvicted = int32(1 << 5)
	// NotifyEntryAllCleared is dispatched if all entries are cleared.
	NotifyEntryAllCleared = int32(1 << 6)
	// NotifyEntryMerged is dispatched if an entry is merged after a network partition.
	NotifyEntryMerged = int32(1 << 7)
	// NotifyEntryInvalidated is dispatched if an entry is invalidated.
	NotifyEntryInvalidated = int32(1 << 8)
	// NotifyEntryLoaded is dispatched if an entry is loaded.
	NotifyEntryLoaded = int32(1 << 9)
)

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

type EntryNotified struct {
	MergingValue            interface{}
	Key                     interface{}
	Value                   interface{}
	OldValue                interface{}
	MapName                 string
	Member                  cluster.MemberInfo
	NumberOfAffectedEntries int
	EventType               int32
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
) *EntryNotified {
	return &EntryNotified{
		MapName:                 mapName,
		Member:                  member,
		Key:                     key,
		Value:                   value,
		OldValue:                oldValue,
		MergingValue:            mergingValue,
		NumberOfAffectedEntries: numberOfAffectedEntries,
	}
}

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
	case LifecycleStateClientConnected:
		return "client connected"
	case LifecycleStateClientDisconnected:
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
	// LifecycleStateClientConnected signals that the client connected to the cluster.
	LifecycleStateClientConnected
	// LifecycleStateClientDisconnected signals that the client disconnected from the cluster.
	LifecycleStateClientDisconnected
)

type LifecycleStateChangeHandler func(event LifecycleStateChanged)

type LifecycleStateChanged struct {
	State LifecycleState
}

func (e *LifecycleStateChanged) EventName() string {
	return eventLifecycleEventStateChanged
}

func newLifecycleStateChanged(state LifecycleState) *LifecycleStateChanged {
	return &LifecycleStateChanged{State: state}
}

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
	// NotifyItemAdded stands for item added event.
	NotifyItemAdded ItemEventType = 1
	// NotifyItemRemoved stands for item removed event.
	NotifyItemRemoved ItemEventType = 2
)

type QueueItemNotifiedHandler func(event *QueueItemNotified)

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

type SetItemNotifiedHandler func(event *SetItemNotified)

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

type DistributedObjectEventType string

const (
	DistributedObjectCreated   DistributedObjectEventType = "CREATED"
	DistributedObjectDestroyed DistributedObjectEventType = "DESTROYED"
)

type DistributedObjectNotifiedHandler func(event DistributedObjectNotified)

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
