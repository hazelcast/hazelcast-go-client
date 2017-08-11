package core

type IMap interface {
	Put(key interface{}, value interface{}) (oldValue interface{}, err error)
	Get(key interface{}) (value interface{}, err error)
}
