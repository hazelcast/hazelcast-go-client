package core

import . "github.com/hazelcast/go-client/internal/protocol"

type IMap interface {
	Put(key interface{}, value interface{}) (oldValue interface{}, err error)
	Get(key interface{}) (value interface{}, err error)
	Remove(key interface{}) (value interface{}, err error)
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
	Lock(key interface{}) error
	UnLock(key interface{}) error
	IsLocked(key interface{}) (bool, error)
	Replace(key interface{}, value interface{}) (interface{}, error)
	ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (bool, error)
	Set(key interface{}, value interface{}) error
	PutIfAbsent(key interface{}, value interface{}) (oldValue interface{}, err error)
	PutAll(mp *map[interface{}]interface{}) error
	EntrySet() ([]Pair, error)
	GetAll(keys []interface{}) (*[]Pair, error)
	GetEntryView(key interface{}) (*EntryView, error)
	AddEntryListener(listener interface{}, includeValue bool) (*string, error)
	RemoveEntryListener(registrationId *string) error
}
