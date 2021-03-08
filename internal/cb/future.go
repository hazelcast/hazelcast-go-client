package cb

// Future holds a value computed in the future
// Futures are not re-usable
type Future interface {
	// Result blocks until the future is complete
	Result() (maybeResult interface{}, err error)
}

type FutureImpl struct {
	resultCh chan interface{}
}

func NewFutureImpl() *FutureImpl {
	return &FutureImpl{
		resultCh: make(chan interface{}, 1),
	}
}

func NewSucceededFuture(result interface{}) *FutureImpl {
	fut := NewFutureImpl()
	fut.resultCh <- result
	return fut
}

func NewFailedFuture(err error) *FutureImpl {
	return NewSucceededFuture(err)
}

func (f FutureImpl) Result() (interface{}, error) {
	result := <-f.resultCh
	switch v := result.(type) {
	case error:
		return nil, v
	default:
		return v, nil
	}
}
