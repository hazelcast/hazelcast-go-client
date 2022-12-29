/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cp/types"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"math"
	"time"
)

/*
proxy is the parent struct of CP Subsystem data structures.
It's exported fields and methods are in public API so directly accessible by users.
Be careful while adding new methods/fields to proxy.
*/
type proxy struct {
	cb         *cb.CircuitBreaker
	groupID    types.RaftGroupId
	invFactory *cluster.ConnectionInvocationFactory
	is         *invocation.Service
	lg         *logger.LogAdaptor
	name       string
	object     string
	service    string
	ss         *iserialization.Service
}

func newProxy(ss *iserialization.Service, invFactory *cluster.ConnectionInvocationFactory, is *invocation.Service, lg *logger.LogAdaptor, svc string, pxy string, obj string) *proxy {
	circuitBreaker := cb.NewCircuitBreaker(
		cb.MaxRetries(math.MaxInt32),
		cb.MaxFailureCount(10),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration((attempt+1)*100) * time.Millisecond
		}))
	p := &proxy{
		cb:         circuitBreaker,
		invFactory: invFactory,
		is:         is,
		lg:         lg,
		name:       pxy,
		object:     obj,
		service:    svc,
		ss:         ss,
	}
	return p
}

func (p *proxy) Name() string {
	return p.name
}

func (p *proxy) ServiceName() string {
	return p.service
}

func (p *proxy) Destroy(ctx context.Context) error {
	request := codec.EncodeCPGroupDestroyCPObjectRequest(p.groupID, p.service, p.object)
	_, err := p.invokeOnRandomTarget(ctx, request, nil)
	return err
}

func (p *proxy) invokeOnRandomTarget(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler) (*proto.ClientMessage, error) {
	now := time.Now()
	if ctx == nil {
		ctx = context.Background()
	}
	response, err := p.cb.TryContext(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt > 0 {
			request = request.Copy()
		}
		inv := p.invFactory.NewInvocationOnRandomTarget(request, handler, now)
		if err := p.is.SendRequest(ctx, inv); err != nil {
			return nil, err
		}
		return inv.GetWithContext(ctx)
	})
	if err != nil {
		return nil, err
	} else {
		return response.(*proto.ClientMessage), nil
	}
}
