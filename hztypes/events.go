package hztypes

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
	// NotifyEntryInvalidation is dispatched if an entry is invalidated.
	NotifyEntryInvalidation = int32(1 << 8)
	// NotifyEntryLoaded is dispatched if an entry is loaded.
	NotifyEntryLoaded = int32(1 << 9)
)

const (
	NotifyItemAdded   int32 = 1
	NotifyItemRemoved int32 = 2
)

type EntryNotifiedHandler func(event *EntryNotified)

const (
	EventEntryNotified = "internal.proxy.entrynotified"
)

type EntryNotified struct {
	EventType               int32
	OwnerName               string
	MemberName              string
	Key                     interface{}
	Value                   interface{}
	OldValue                interface{}
	MergingValue            interface{}
	NumberOfAffectedEntries int
}

func (e *EntryNotified) EventName() string {
	return EventEntryNotified
}

func newEntryNotifiedEventImpl(
	ownerName string,
	memberName string,
	key interface{},
	value interface{},
	oldValue interface{},
	mergingValue interface{},
	numberOfAffectedEntries int,
) *EntryNotified {
	return &EntryNotified{
		OwnerName:               ownerName,
		MemberName:              memberName,
		Key:                     key,
		Value:                   value,
		OldValue:                oldValue,
		MergingValue:            mergingValue,
		NumberOfAffectedEntries: numberOfAffectedEntries,
	}
}
