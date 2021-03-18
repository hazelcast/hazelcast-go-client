package hztypes

type Map interface {
	Get(key interface{}) (interface{}, error)
	Put(key interface{}, value interface{}) (interface{}, error)
	ListenEntryNotified(flags int32, includeValue bool, handler EntryNotifiedHandler) error
}
