/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hazelcast

import (
	"context"
	"sync"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/types"
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
guarantees are lost and the method invocation will throw an
hzerrors.HazelcastConsistencyLostError. This does not mean
that an update is lost. All of the updates are part of some replica and
will be eventually reflected in the state of all other replicas. This
exception just means that you cannot observe your own writes because
all replicas that contain your updates are currently unreachable.
After you have received an hzerrors.HazelcastConsistencyLostError, you can either
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
	target *cluster.MemberInfo
	mu     *sync.Mutex
}

func newPNCounter(p *proxy) *PNCounter {
	return &PNCounter{
		proxy: p,
		clock: iproxy.NewVectorClock(),
		mu:    &sync.Mutex{},
	}
}

// AddAndGet adds the given value to the current value and returns the updated value.
func (pn *PNCounter) AddAndGet(ctx context.Context, delta int64) (int64, error) {
	return pn.add(ctx, delta, false)
}

// DecrementAndGet decrements the counter value by one and returns the updated value.
func (pn *PNCounter) DecrementAndGet(ctx context.Context) (int64, error) {
	return pn.add(ctx, -1, false)
}

// Get returns the current value of the counter.
func (pn *PNCounter) Get(ctx context.Context) (int64, error) {
	resp, err := pn.invokeOnMember(ctx, func(uuid types.UUID, clocks []proto.Pair) *proto.ClientMessage {
		return codec.EncodePNCounterGetRequest(pn.name, clocks, uuid)
	})
	if err != nil {
		return 0, err
	}
	value, timestamps, _ := codec.DecodePNCounterGetResponse(resp)
	pn.updateClock(iproxy.NewVectorClockFromPairs(timestamps))
	return value, nil
}

// GetAndAdd adds the given value to the current value and returns the previous value.
func (pn *PNCounter) GetAndAdd(ctx context.Context, delta int64) (int64, error) {
	return pn.add(ctx, delta, true)
}

// GetAndDecrement decrements the counter value by one and returns the previous value.
func (pn *PNCounter) GetAndDecrement(ctx context.Context) (int64, error) {
	return pn.add(ctx, -1, true)
}

// GetAndIncrement increments the counter value by one and returns the previous value.
func (pn *PNCounter) GetAndIncrement(ctx context.Context) (int64, error) {
	return pn.add(ctx, 1, true)
}

// GetAndSubtract subtracts the given value from the current value and returns the previous value.
func (pn *PNCounter) GetAndSubtract(ctx context.Context, delta int64) (int64, error) {
	return pn.add(ctx, -1*delta, true)
}

// IncrementAndGet increments the counter value by one and returns the updated value.
func (pn *PNCounter) IncrementAndGet(ctx context.Context) (int64, error) {
	return pn.add(ctx, 1, false)
}

// Reset resets the observed state by this PN counter.
func (pn *PNCounter) Reset() {
	pn.mu.Lock()
	pn.clock = iproxy.NewVectorClock()
	pn.mu.Unlock()
}

// SubtractAndGet subtracts the given value from the current value and returns the updated value.
func (pn *PNCounter) SubtractAndGet(ctx context.Context, delta int64) (int64, error) {
	return pn.add(ctx, -1*delta, false)
}

func (pn *PNCounter) crdtOperationTarget(excluded map[cluster.Address]struct{}) (*cluster.MemberInfo, []proto.Pair, error) {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	target := pn.target
	if target == nil || targetExcluded(pn.target, excluded) {
		if excluded == nil {
			target = pn.clusterService.RandomDataMember()
		} else {
			target = pn.clusterService.RandomDataMemberExcluding(excluded)
		}
		if target == nil {
			return nil, nil, ihzerrors.NewClientError("no data members in cluster", nil, hzerrors.ErrNoDataMember)
		}
		pn.target = target
	}
	entries := pn.clock.EntrySet()
	return target, entries, nil
}

func (pn *PNCounter) updateClock(clock iproxy.VectorClock) {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	// TODO: implement this properly
	if pn.clock.After(clock) {
		return
	}
	pn.clock = clock
}

func (pn *PNCounter) add(ctx context.Context, delta int64, getBeforeUpdate bool) (int64, error) {
	resp, err := pn.invokeOnMember(ctx, func(uuid types.UUID, clocks []proto.Pair) *proto.ClientMessage {
		return codec.EncodePNCounterAddRequest(pn.name, delta, getBeforeUpdate, clocks, uuid)
	})
	if err != nil {
		return 0, err
	}
	value, timestamps, _ := codec.DecodePNCounterAddResponse(resp)
	pn.updateClock(iproxy.NewVectorClockFromPairs(timestamps))
	return value, nil
}

func (pn *PNCounter) invokeOnMember(ctx context.Context, makeReq func(target types.UUID, clocks []proto.Pair) *proto.ClientMessage) (*proto.ClientMessage, error) {
	// in the best case scenario, no members will be excluded, so excluded set is nil
	var excluded map[cluster.Address]struct{}
	var lastAddr cluster.Address
	var request *proto.ClientMessage
	return pn.tryInvoke(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt == 1 {
			// this is the first failure, time to allocate the excluded set
			excluded = map[cluster.Address]struct{}{}
		}
		if attempt > 0 {
			request = request.Copy()
			excluded[lastAddr] = struct{}{}
		}
		mem, clocks, err := pn.crdtOperationTarget(excluded)
		if err != nil {
			// do not retry if no data members was found
			return nil, cb.WrapNonRetryableError(err)
		}
		request = makeReq(mem.UUID, clocks)
		inv := pn.invocationFactory.NewMemberBoundInvocation(request, mem)
		if err := pn.sendInvocation(ctx, inv); err != nil {
			return nil, err
		}
		return inv.GetWithContext(ctx)
	})
}

func targetExcluded(target *cluster.MemberInfo, excludes map[cluster.Address]struct{}) bool {
	if excludes == nil {
		return false
	}
	_, found := excludes[target.Address]
	return found
}
