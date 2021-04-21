package hazelcast

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
	EventEntryNotified              = "entrynotified"
	EventLifecycleEventStateChanged = "lifecyclestatechanged"
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

type LifecycleState int

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
	return EventLifecycleEventStateChanged
}

func newLifecycleStateChanged(state LifecycleState) *LifecycleStateChanged {
	return &LifecycleStateChanged{State: state}
}
