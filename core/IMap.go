package core

type IMap interface {
	Put(key interface{}, value interface{}) (oldValue interface{}, err error)
	Get(key interface{}) (value interface{}, err error)
    Remove(key interface{}) (value interface{}, err error)
    Size() (size interface{},err error)
	ContainsKey(key interface{}) (found interface{},err error)
}
