package proxy

const MapServiceName = "hz:impl:mapService"

type Map interface {
	Get(key interface{}) (interface{}, error)
}
