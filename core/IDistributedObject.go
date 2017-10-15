package core

type IDistributedObject interface {
	Destroy() bool
	Name() string
	PartitionKey() string
	ServiceName() string
}
