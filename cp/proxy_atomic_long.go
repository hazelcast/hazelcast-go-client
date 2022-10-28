package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

type AtomicLong struct {
	*proxy
}

func newAtomicLong(p *proxy) *AtomicLong {
	return &AtomicLong{proxy: p}
}

func (al *AtomicLong) AddAndGet(delta int64) error {
	request := codec.EncodeAtomicLongAddAndGetRequest(al.groupId, al.proxyName, delta)
	if _, err := al.invokeOnRandomTarget(context.Background(), request, nil); err != nil {
		return err
	}
	return nil
}

func (al *AtomicLong) CompareAndSet() {

}

func (al *AtomicLong) DecrementAndGet() {

}

func (al *AtomicLong) Get() (interface{}, error) {
	request := codec.EncodeAtomicLongGetRequest(al.groupId, al.proxyName)
	if response, err := al.invokeOnRandomTarget(context.Background(), request, nil); err != nil {
		return nil, err
	} else {
		return codec.DecodeAtomicLongGetResponse(response), nil
	}
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
