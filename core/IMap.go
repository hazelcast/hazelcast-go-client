package core

import (
	"time"
)

type IMap interface {
	Put(key interface{}, value interface{}) (oldValue interface{}, err error)
	Get(key interface{}) (value interface{}, err error)
	Remove(key interface{}) (value interface{}, err error)
	RemoveIfSame(key interface{}, value interface{}) (ok bool, err error)
	Size() (size int32, err error)
	ContainsKey(key interface{}) (found bool, err error)
	ContainsValue(value interface{}) (found bool, err error)
	Clear() (err error)
	Delete(key interface{}) (err error)
	IsEmpty() (empty bool, err error)
	AddIndex(attributes *string, ordered bool) (err error)
	Evict(key interface{}) (bool, error)
	EvictAll() error
	Flush() error
	ForceUnlock(key interface{}) error
	Lock(key interface{}) error
	LockWithLeaseTime(key interface{}, lease int64, leaseTimeUnit time.Duration) error
	UnLock(key interface{}) error
	IsLocked(key interface{}) (bool, error)
	Replace(key interface{}, value interface{}) (interface{}, error)
	ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (bool, error)
	Set(key interface{}, value interface{}) error
	SetWithTtl(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) error
	PutIfAbsent(key interface{}, value interface{}) (oldValue interface{}, err error)
	PutAll(mp *map[interface{}]interface{}) error
	EntrySet() ([]IPair, error)
	TryLock(key interface{}) (bool, error)
	TryLockWithTimeout(key interface{}, timeout int64, timeoutTimeUnit time.Duration) (bool, error)
	TryLockWithTimeoutAndLease(key interface{}, timeout int64, timeoutTimeUnit time.Duration, lease int64, leaseTimeUnit time.Duration) (bool, error)
	TryPut(key interface{}, value interface{}) (ok bool, err error)
	TryRemove(key interface{}, timeout int64, timeoutTimeUnit time.Duration) (ok bool, err error)
	GetAll(keys []interface{}) ([]IPair, error)
	GetEntryView(key interface{}) (IEntryView, error)
	PutTransient(key interface{}, value interface{}, ttl int64, ttlTimeUnit time.Duration) (err error)
	AddEntryListener(listener interface{}, includeValue bool) (*string, error)
	AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (*string, error)
	RemoveEntryListener(registrationId *string) error
}
