package invocation

type Handler interface {
	Invoke(invocation Invocation) error
}
