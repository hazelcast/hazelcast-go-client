package core

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
}
