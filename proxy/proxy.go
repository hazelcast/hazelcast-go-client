package proxy

type Map interface {
	Get(key interface{}) (interface{}, error)
	Put(key interface{}, value interface{}) (interface{}, error)
}
