/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package client

import (
	"context"
	"fmt"
	"math"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type CRDTOperationTargetFn func(excluded map[types.UUID]struct{}) (*pubcluster.MemberInfo, []proto.Pair)

type Invoker struct {
	factory *cluster.ConnectionInvocationFactory
	svc     *invocation.Service
	CB      *cb.CircuitBreaker
	lg      *logger.LogAdaptor
}

func NewInvoker(factory *cluster.ConnectionInvocationFactory, svc *invocation.Service, lg *logger.LogAdaptor) *Invoker {
	cbr := cb.NewCircuitBreaker(
		cb.MaxRetries(math.MaxInt32),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration((attempt+1)*100) * time.Millisecond
		}),
	)
	return &Invoker{
		factory: factory,
		svc:     svc,
		CB:      cbr,
		lg:      lg,
	}
}

func (iv *Invoker) InvokeOnConnection(ctx context.Context, req *proto.ClientMessage, conn *cluster.Connection) (*proto.ClientMessage, error) {
	now := time.Now()
	return iv.TryInvoke(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt > 0 {
			req = req.Copy()
		}
		inv := iv.factory.NewConnectionBoundInvocation(req, conn, nil, now)
		if err := iv.svc.SendRequest(ctx, inv); err != nil {
			return nil, err
		}
		return inv.GetWithContext(ctx)
	})
}

func (iv *Invoker) InvokeOnPartition(ctx context.Context, request *proto.ClientMessage, partitionID int32) (*proto.ClientMessage, error) {
	now := time.Now()
	return iv.TryInvoke(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		inv, err := iv.InvokeOnPartitionAsync(ctx, request, partitionID, now)
		if err != nil {
			return nil, err
		}
		return inv.GetWithContext(ctx)
	})
}

func (iv *Invoker) InvokeOnPartitionAsync(ctx context.Context, request *proto.ClientMessage, partitionID int32, now time.Time) (invocation.Invocation, error) {
	inv := iv.factory.NewInvocationOnPartitionOwner(request, partitionID, now)
	err := iv.SendInvocation(ctx, inv)
	return inv, err
}

func (iv *Invoker) InvokeOnRandomTarget(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler) (*proto.ClientMessage, error) {
	return iv.invokeOnRandomTarget(ctx, request, handler, false)
}

func (iv *Invoker) InvokeUrgentOnRandomTarget(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler) (*proto.ClientMessage, error) {
	return iv.invokeOnRandomTarget(ctx, request, handler, true)
}

func (iv *Invoker) invokeOnRandomTarget(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler, urgent bool) (*proto.ClientMessage, error) {
	now := time.Now()
	return iv.TryInvoke(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt > 0 {
			request = request.Copy()
		}
		inv := iv.factory.NewInvocationOnRandomTarget(request, handler, now)
		var err error
		if urgent {
			err = iv.svc.SendUrgentRequest(ctx, inv)
		} else {
			err = iv.svc.SendRequest(ctx, inv)
		}
		if err != nil {
			return nil, err
		}
		return inv.GetWithContext(ctx)
	})
}

func (iv *Invoker) InvokeOnMemberCRDT(ctx context.Context, makeReq func(target types.UUID, clocks []proto.Pair) *proto.ClientMessage, crdtFn CRDTOperationTargetFn) (*proto.ClientMessage, error) {
	// in the best case scenario, no members will be excluded, so excluded set is nil
	var excluded map[types.UUID]struct{}
	var lastUUID types.UUID
	var request *proto.ClientMessage
	now := time.Now()
	return iv.TryInvoke(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt == 1 {
			// this is the first failure, time to allocate the excluded set
			excluded = map[types.UUID]struct{}{}
		}
		if attempt > 0 {
			request = request.Copy()
			excluded[lastUUID] = struct{}{}
		}
		mem, clocks := crdtFn(excluded)
		if mem == nil {
			iv.lg.Debug(func() string {
				return fmt.Sprintf("attempt: %d, excluded members: %v", attempt, excluded)
			})
			// do not retry if no data members was found
			err := ihzerrors.NewClientError("no data members in cluster", nil, hzerrors.ErrNoDataMember)
			return nil, cb.WrapNonRetryableError(err)
		}
		lastUUID = mem.UUID
		request = makeReq(mem.UUID, clocks)
		inv := iv.factory.NewMemberBoundInvocation(request, mem, now)
		if err := iv.SendInvocation(ctx, inv); err != nil {
			return nil, err
		}
		return inv.GetWithContext(ctx)
	})
}

func (iv *Invoker) Factory() *cluster.ConnectionInvocationFactory {
	return iv.factory
}

func (iv *Invoker) SendInvocation(ctx context.Context, inv invocation.Invocation) error {
	return iv.svc.SendRequest(ctx, inv)
}

func (iv *Invoker) TryInvoke(ctx context.Context, f cb.TryHandler) (*proto.ClientMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	res, err := iv.CB.TryContext(ctx, f)
	if err != nil {
		return nil, err
	}
	return res.(*proto.ClientMessage), nil
}
