package internal

type EndpointQualifier struct {
	_type      int32
	identifier string
}

func NewEndpointQualifier(_type int32, identifier string) EndpointQualifier {
	return EndpointQualifier{_type, identifier}
}

func (e EndpointQualifier) Type() int32 {
	return e._type
}

func (e EndpointQualifier) Identifier() string {
	return e.identifier
}
