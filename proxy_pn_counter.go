package hazelcast

import (
	"context"
	"errors"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	icluster "github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
)

/*
PNCounter is a PN (Positive-Negative) CRDT counter.

The counter supports adding and subtracting values as well as
retrieving the current counter value.
Each replica of this counter can perform operations locally without
coordination with the other replicas, thus increasing availability.
The counter guarantees that whenever two nodes have received the
same set of updates, possibly in a different order, their state is
identical, and any conflicting updates are merged automatically.
If no new updates are made to the shared state, all nodes that can
communicate will eventually have the same data.

When invoking updates from the client, the invocation is remote.
This may lead to indeterminate state - the update may be applied but the
response has not been received. In this case, the caller will be notified
with a TargetDisconnectedError.

The read and write methods provide monotonic read and RYW (read-your-write)
guarantees. These guarantees are session guarantees which means that if
no replica with the previously observed state is reachable, the session
guarantees are lost and the method invocation will throw a
ConsistencyLostError. This does not mean
that an update is lost. All of the updates are part of some replica and
will be eventually reflected in the state of all other replicas. This
exception just means that you cannot observe your own writes because
all replicas that contain your updates are currently unreachable.
After you have received a ConsistencyLostError, you can either
wait for a sufficiently up-to-date replica to become reachable in which
case the session can be continued or you can reset the session by calling
the reset() method. If you have called the reset() method,
a new session is started with the next invocation to a CRDT replica.

Notes:
The CRDT state is kept entirely on non-lite (data) members. If there
aren't any and the methods here are invoked on a lite member, they will
fail with an NoDataMemberInClusterError.
*/
type PNCounter struct {
	*proxy
	clock  iproxy.VectorClock
	target *icluster.Member
}

func newPNCounter(p *proxy) *PNCounter {
	return &PNCounter{
		proxy: p,
		clock: iproxy.NewVectorClock(),
	}
}

// AddAndGet adds the given value to the current value and returns the updated value.
func (pn *PNCounter) AddAndGet(ctx context.Context, delta int64) (int64, error) {
	target, err := pn.crdtOperationTarget()
	if err != nil {
		return 0, err
	}
	request := codec.EncodePNCounterAddRequest(pn.name, delta, false, pn.clock.EntrySet(), target.UUID())
	resp, err := pn.invokeOnTarget(ctx, request, target.Address().(*cluster.AddressImpl))
	if err != nil {
		return 0, err
	}
	value, timestamps, _ := codec.DecodePNCounterAddResponse(resp)
	pn.updateClock(iproxy.NewVectorClockFromPairs(timestamps))
	return value, nil
}

// Get returns the current value of the counter.
func (pn *PNCounter) Get(ctx context.Context) (int64, error) {
	target, err := pn.crdtOperationTarget()
	if err != nil {
		return 0, err
	}
	request := codec.EncodePNCounterGetRequest(pn.name, pn.clock.EntrySet(), target.UUID())
	resp, err := pn.invokeOnTarget(ctx, request, target.Address().(*cluster.AddressImpl))
	if err != nil {
		return 0, err
	}
	value, timestamps, _ := codec.DecodePNCounterGetResponse(resp)
	pn.updateClock(iproxy.NewVectorClockFromPairs(timestamps))
	return value, nil
}

func (pn *PNCounter) crdtOperationTarget() (*icluster.Member, error) {
	if pn.target == nil {
		mem := pn.clusterService.RandomDataMember()
		if mem == nil {
			// TODO: use the correct error
			return nil, errors.New("crdt target mem is nil")
		}
		pn.target = mem
	}
	return pn.target, nil
}

func (pn *PNCounter) updateClock(clock iproxy.VectorClock) {
	// TODO: implement this properly
	if pn.clock.After(clock) {
		return
	}
	pn.clock = clock
}
