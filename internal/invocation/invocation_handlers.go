package invocation

type Handler interface {
	Invoke(invocation Invocation) error
}

type DefaultHandler struct {
}

func (d DefaultHandler) Invoke(invocation Invocation) error {
	panic("implement me")
}
