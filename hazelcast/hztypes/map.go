package hztypes

import (
	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/pred"
)

type MapEntryListenerConfig struct {
	Predicate          pred.Predicate
	IncludeValue       bool
	Key                interface{}
	NotifyEntryAdded   bool
	NotifyEntryRemoved bool
	NotifyEntryUpdated bool
}

type Map interface {
	// AddIndex adds an index to this map for the specified entries so that queries can run faster.
	AddIndex(indexConfig IndexConfig) error

	// AddInterceptor adds an interceptor for this map.
	AddInterceptor(interceptor interface{}) (string, error)

	// Clear deletes all entries one by one and fires related events
	Clear() error

	// ContainsKey returns true if the map contains an entry with the given key
	ContainsKey(key interface{}) (bool, error)

	// ContainsValue returns true if the map contains an entry with the given value
	ContainsValue(value interface{}) (bool, error)

	// Delete removes the mapping for a key from this map if it is present
	// Unlike remove(object), this operation does not return the removed value, which avoids the serialization cost of
	// the returned value. If the removed value will not be used, a delete operation is preferred over a remove
	// operation for better performance.
	Delete(key interface{}) error

	// Evict evicts the mapping for a key from this map.
	// Returns true if the key is evicted.
	Evict(key interface{}) (bool, error)

	// EvictAll deletes all entries withour firing releated events
	EvictAll() error

	// ExecuteOnEntries pplies the user defined EntryProcessor to all the entries in the map.
	ExecuteOnEntries(entryProcessor interface{}) ([]Entry, error)

	// Flush flushes all the local dirty entries.
	Flush() error

	// ForceUnlock releases the lock for the specified key regardless of the lock owner.
	// It always successfully unlocks the key, never blocks, and returns immediately.
	ForceUnlock(key interface{}) error

	// Get returns the value for the specified key, or nil if this map does not contain this key.
	// Warning:
	//   This method returns a clone of original value, modifying the returned value does not change the
	//   actual value in the map. One should put modified value back to make changes visible to all nodes.
	Get(key interface{}) (interface{}, error)

	// GetAll returns the entries for the given keys.
	GetAll(keys ...interface{}) ([]Entry, error)

	// GetKeySet returns keys contained in this map
	GetKeySet() ([]interface{}, error)

	// GetValues returns a list clone of the values contained in this map
	GetValues() ([]interface{}, error)

	// GetEntryView returns the SimpleEntryView for the specified key.
	GetEntryView(key string) (*SimpleEntryView, error)

	// GetEntrySet returns a clone of the mappings contained in this map.
	GetEntrySet() ([]Entry, error)

	// GetEntrySetWithPredicate returns a clone of the mappings contained in this map.
	GetEntrySetWithPredicate(predicate pred.Predicate) ([]Entry, error)

	// IsEmpty returns true if this map contains no key-value mappings.
	IsEmpty() (bool, error)

	// IsLocked checks the lock for the specified key.
	IsLocked(key interface{}) (bool, error)

	// ListenEntryNotification adds a continuous entry listener to this map.
	ListenEntryNotification(config MapEntryListenerConfig, handler EntryNotifiedHandler) error

	// LoadAll loads all keys from the store at server side or loads the given keys if provided.
	LoadAll(keys ...interface{}) error

	// LoadAll loads all keys from the store at server side or loads the given keys if provided.
	// Replaces existing keys.
	LoadAllReplacingExisting(keys ...interface{}) error

	// Lock acquires the lock for the specified key infinitely or for the specified lease time if provided.
	// If the lock is not available, the current thread becomes disabled for thread scheduling purposes and lies
	// dormant until the lock has been acquired.
	//
	// You get a lock whether the value is present in the map or not. Other threads (possibly on other systems) would
	// block on their invoke of lock() until the non-existent key is unlocked. If the lock holder introduces the key to
	// the map, the put() operation is not blocked. If a thread not holding a lock on the non-existent key tries to
	// introduce the key while a lock exists on the non-existent key, the put() operation blocks until it is unlocked.
	//
	// Scope of the lock is this map only. Acquired lock is only for the key in this map.
	//
	// Locks are re-entrant; so, if the key is locked N times, it should be unlocked N times before another thread can
	// acquire it.
	Lock(key interface{}) error

	// Put sets the value for the given key and returns the old value.
	Put(key interface{}, value interface{}) (interface{}, error)

	// PutALl copies all of the mappings from the specified map to this map.
	// No atomicity guarantees are given. In the case of a failure, some of the key-value tuples may get written,
	// while others are not.
	PutAll(keyValuePairs []Entry) error

	// PutIfAbsent associates the specified key with the given value if it is not already associated.
	PutIfAbsent(key interface{}, value interface{}) (interface{}, error)

	// PutIfAbsent associates the specified key with the given value if it is not already associated.
	// Entry will expire and get evicted after the ttl.
	PutIfAbsentWithTTL(key interface{}, value interface{}, ttl time.Duration) (interface{}, error)

	// PutTransient sets the value for the given key.
	// MapStore defined at the server side will not be called.
	// The TTL defined on the server-side configuration will be used.
	// Max idle time defined on the server-side configuration will be used.
	PutTransient(key interface{}, value interface{}) error

	// PutTransient sets the value for the given key.
	// MapStore defined at the server side will not be called.
	// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
	// Set ttl to 0 for infinite timeout.
	PutTransientWithTTL(key interface{}, value interface{}, ttl time.Duration) error

	// PutTransient sets the value for the given key.
	// MapStore defined at the server side will not be called.
	// Given max idle time (maximum time for this entry to stay idle in the map) is used.
	// Set maxIdle to 0 for infinite idle time.
	PutTransientWithMaxIdle(key interface{}, value interface{}, maxIdle time.Duration) error

	// PutTransient sets the value for the given key.
	// MapStore defined at the server side will not be called.
	// Given TTL (maximum time in seconds for this entry to stay in the map) is used.
	// Set ttl to 0 for infinite timeout.
	// Given max idle time (maximum time for this entry to stay idle in the map) is used.
	// Set maxIdle to 0 for infinite idle time.
	PutTransientWithTTLMaxIdle(key interface{}, value interface{}, ttl time.Duration, maxIdle time.Duration) error

	// Remove deletes the value for the given key and returns it.
	Remove(key interface{}) (interface{}, error)

	// RemoveIfSame removes the entry for a key only if it is currently mapped to a given value.
	// Returns true if the entry was removed.
	RemoveIfSame(key interface{}, value interface{}) (bool, error)

	// Replace replaces the entry for a key only if it is currently mapped to some value and returns the previous value.
	Replace(key interface{}, value interface{}) (interface{}, error)

	// ReplaceIfSame replaces the entry for a key only if it is currently mapped to a given value.
	// Returns true if the value was replaced.
	ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (bool, error)

	// Set sets the value for the given key and returns the old value.
	Set(key interface{}, value interface{}) error

	// SetWithTTL
	SetWithTTL(key interface{}, value interface{}, ttl time.Duration) error

	// Size returns the number of entries in this map.
	Size() (int, error)

	// TryLock tries to acquire the lock for the specified key.
	// When the lock is not available, the current thread doesn't wait and returns false immediately.
	TryLock(key interface{}) (bool, error)

	// TryLockWithLease tries to acquire the lock for the specified key.
	// Lock will be released after lease time passes.
	TryLockWithLease(key interface{}, lease time.Duration) (bool, error)

	// TryLockWithLease tries to acquire the lock for the specified key.
	// The current thread becomes disabled for thread scheduling purposes and lies
	// dormant until one of the followings happens:
	// - The lock is acquired by the current thread, or
	// - The specified waiting time elapses.
	TryLockWithTimeout(key interface{}, timeout time.Duration) (bool, error)

	// TryLockWithLease tries to acquire the lock for the specified key.
	// The current thread becomes disabled for thread scheduling purposes and lies
	// dormant until one of the followings happens:
	// - The lock is acquired by the current thread, or
	// - The specified waiting time elapses.
	// Lock will be released after lease time passes.
	TryLockWithLeaseTimeout(key interface{}, lease time.Duration, timeout time.Duration) (bool, error)

	// TryPut tries to put the given key and value into this map and returns immediately.
	TryPut(key interface{}, value interface{}) (interface{}, error)

	// TryPut tries to put the given key and value into this map and waits until operation is completed or timeout is reached.
	TryPutWithTimeout(key interface{}, value interface{}, timeout time.Duration) (interface{}, error)

	// TryRemove tries to remove the given key from this map and returns immediately.
	TryRemove(key interface{}) (interface{}, error)

	// TryRemove tries to remove the given key from this map and waits until operation is completed or timeout is reached.
	TryRemoveWithTimeout(key interface{}, timeout time.Duration) (interface{}, error)

	// UnlistenEntryNotification removes the specified entry listener.
	UnlistenEntryNotification(handler EntryNotifiedHandler) error

	// Unlock releases the lock for the specified key.
	Unlock(key interface{}) error
}
