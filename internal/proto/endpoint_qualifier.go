package proto

type EndpointQualifier struct {
	_type      int32
	identifier string
}

func NewEndpointQualifier(_type int32, identifier string) EndpointQualifier {
	return EndpointQualifier{_type, identifier}
}

func (e EndpointQualifier) GetType() int32 {
	return e._type
}

func (e EndpointQualifier) GetIdentifier() string {
	return e.identifier
}
