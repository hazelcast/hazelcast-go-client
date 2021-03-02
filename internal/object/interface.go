package object

type Object interface {
	Name() string
	ServiceName() string
	PartitionKey() string
}
