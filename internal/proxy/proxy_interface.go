package proxy

type Proxy interface {
	Destroy() error
	Smart() bool
	Name() string
	ServiceName() string
	PartitionKey() string
}
