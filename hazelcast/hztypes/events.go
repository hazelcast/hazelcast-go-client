package hztypes

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
)

const (
	NotifyEntryAdded        int32 = bufutil.EntryEventAdded
	NotifyEntryRemoved      int32 = bufutil.EntryEventRemoved
	NotifyEntryUpdated      int32 = bufutil.EntryEventUpdated
	NotifyEntryEvicted      int32 = bufutil.EntryEventEvicted
	NotifyEntryMerged       int32 = bufutil.EntryEventMerged
	NotifyEntryExpired      int32 = bufutil.EntryEventExpired
	NotifyEntryInvalidation int32 = bufutil.EntryEventInvalidation
	NotifyEntryLoaded       int32 = bufutil.EntryEventLoaded
	NotifyMapEvicted        int32 = bufutil.MapEventEvicted
	NotifyMapCleared        int32 = bufutil.MapEventCleared
)

type EntryNotifiedEvent interface {
	event.Event
	EntryEventType() int32
	OwnerName() string
	MemberName() string
	Key() interface{}
	Value() interface{}
	OldValue() interface{}
	MergingValue() interface{}
}

type EntryNotifiedHandler func(event EntryNotifiedEvent)
