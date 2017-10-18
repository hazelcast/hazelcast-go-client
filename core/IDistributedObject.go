package core

type IDistributedObject interface {
	Destroy() (bool,error)
	Name() string
	PartitionKey() string
	ServiceName() string
}
