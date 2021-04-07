package hztypes

import "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/pred"

type ReplicatedMapEntryListenerConfig struct {
	Predicate pred.Predicate
	Key       interface{}
}

type ReplicatedMap interface {
	// Clear deletes all entries one by one and fires related events
	Clear() error

	// ContainsKey returns true if the map contains an entry with the given key
	ContainsKey(key interface{}) (bool, error)

	// ContainsValue returns true if the map contains an entry with the given value
	ContainsValue(value interface{}) (bool, error)

	// Get returns the value for the specified key, or nil if this map does not contain this key.
	// Warning:
	//   This method returns a clone of original value, modifying the returned value does not change the
	//   actual value in the map. One should put modified value back to make changes visible to all nodes.
	Get(key interface{}) (interface{}, error)

	// GetEntrySet returns a clone of the mappings contained in this map.
	GetEntrySet() ([]Entry, error)

	// GetKeySet returns keys contained in this map
	GetKeySet() ([]interface{}, error)

	// GetValues returns a list clone of the values contained in this map
	GetValues() ([]interface{}, error)

	// IsEmpty returns true if this map contains no key-value mappings.
	IsEmpty() (bool, error)

	// ListenEntryNotification adds a continuous entry listener to this map.
	ListenEntryNotification(handler EntryNotifiedHandler) error

	// ListenEntryNotification adds a continuous entry listener to this map.
	ListenEntryNotificationWithConfig(config ReplicatedMapEntryListenerConfig, handler EntryNotifiedHandler) error

	// Put sets the value for the given key and returns the old value.
	Put(key interface{}, value interface{}) (interface{}, error)

	// PutALl copies all of the mappings from the specified map to this map.
	// No atomicity guarantees are given. In the case of a failure, some of the key-value tuples may get written,
	// while others are not.
	PutAll(keyValuePairs []Entry) error

	// Remove deletes the value for the given key and returns it.
	Remove(key interface{}) (interface{}, error)

	// Size returns the number of entries in this map.
	Size() (int, error)

	// UnlistenEntryNotification removes the specified entry listener.
	UnlistenEntryNotification(handler EntryNotifiedHandler) error
}
