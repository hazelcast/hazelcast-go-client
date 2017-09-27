package core

type IList interface {
	IDistributedObject
	Add(element interface{}) bool
	Get(index int32)
}
