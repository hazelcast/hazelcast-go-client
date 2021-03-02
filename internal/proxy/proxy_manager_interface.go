package proxy

type Manager interface {
	GetMap(name string) (Map, error)
	Remove(serviceName string, objectName string) error
}
