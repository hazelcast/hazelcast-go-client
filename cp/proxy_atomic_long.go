package cp

type AtomicLong struct {
	*proxy
}

func NewAtomicLong(p *proxy) *AtomicLong {
	return &AtomicLong{proxy: p}
}

func (al *AtomicLong) AddAndGet() {

}

func (al *AtomicLong) CompareAndSet() {

}

func (al *AtomicLong) DecrementAndGet() {

}

func (al *AtomicLong) Get() {

}

func (al *AtomicLong) GetAndAdd() {

}

func (al *AtomicLong) GetAndDecrement() {

}

func (al *AtomicLong) GetAndSet() {

}

func (al *AtomicLong) IncrementAndGet() {

}

func (al *AtomicLong) Set() {

}
