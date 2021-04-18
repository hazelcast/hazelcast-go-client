// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hazelcast

import (
	"context"
	"fmt"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/util/colutil"
	"github.com/hazelcast/hazelcast-go-client/internal/util/nilutil"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	TtlDefault     = -1
	TtlUnlimited   = 0
	MaxIdleDefault = -1
)

type CreationBundle struct {
	RequestCh            chan<- invocation.Invocation
	SerializationService iserialization.SerializationService
	PartitionService     *cluster.PartitionService
	UserEventDispatcher  *event.DispatchService
	ClusterService       *cluster.ServiceImpl
	InvocationFactory    *cluster.ConnectionInvocationFactory
	ListenerBinder       *cluster.ConnectionListenerBinderImpl
	SmartRouting         bool
	Logger               logger.Logger
}

func (b CreationBundle) Check() {
	if b.RequestCh == nil {
		panic("RequestCh is nil")
	}
	if b.SerializationService == nil {
		panic("SerializationService is nil")
	}
	if b.PartitionService == nil {
		panic("PartitionService is nil")
	}
	if b.UserEventDispatcher == nil {
		panic("UserEventDispatcher is nil")
	}
	if b.ClusterService == nil {
		panic("ClusterService is nil")
	}
	if b.InvocationFactory == nil {
		panic("ConnectionInvocationFactory is nil")
	}
	if b.ListenerBinder == nil {
		panic("ListenerBinder is nil")
	}
	if b.Logger == nil {
		panic("Logger is nil")
	}
}

type proxy struct {
	requestCh            chan<- invocation.Invocation
	serializationService iserialization.SerializationService
	partitionService     *cluster.PartitionService
	userEventDispatcher  *event.DispatchService
	clusterService       *cluster.ServiceImpl
	invocationFactory    *cluster.ConnectionInvocationFactory
	listenerBinder       *cluster.ConnectionListenerBinderImpl
	smartRouting         bool
	serviceName          string
	name                 string
	logger               logger.Logger
	circuitBreaker       *cb.CircuitBreaker
}

func NewProxy(bundle CreationBundle, serviceName string, objectName string) *proxy {
	bundle.Check()
	// TODO: make circuit breaker configurable
	circuitBreaker := cb.NewCircuitBreaker(
		cb.MaxRetries(10),
		cb.MaxFailureCount(10),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration((attempt+1)*100) * time.Millisecond
		}))
	return &proxy{
		serviceName:          serviceName,
		name:                 objectName,
		requestCh:            bundle.RequestCh,
		serializationService: bundle.SerializationService,
		userEventDispatcher:  bundle.UserEventDispatcher,
		partitionService:     bundle.PartitionService,
		clusterService:       bundle.ClusterService,
		invocationFactory:    bundle.InvocationFactory,
		listenerBinder:       bundle.ListenerBinder,
		smartRouting:         bundle.SmartRouting,
		logger:               bundle.Logger,
		circuitBreaker:       circuitBreaker,
	}
}

func (p *proxy) destroy() error {
	request := proto.ClientDestroyProxyEncodeRequest(p.name, p.serviceName)
	inv := p.invocationFactory.NewInvocationOnRandomTarget(request, nil)
	p.requestCh <- inv
	if _, err := inv.Get(); err != nil {
		return fmt.Errorf("error destroying proxy: %w", err)
	}
	return nil
}

func (p *proxy) validateAndSerialize(arg1 interface{}) (arg1Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) {
		return nil, hzerror.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.serializationService.ToData(arg1)
	return
}

func (p *proxy) validateAndSerialize2(arg1 interface{}, arg2 interface{}) (arg1Data serialization.Data,
	arg2Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) || nilutil.IsNil(arg2) {
		return nil, nil, hzerror.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.serializationService.ToData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = p.serializationService.ToData(arg2)
	return
}

func (p *proxy) validateAndSerialize3(arg1 interface{}, arg2 interface{}, arg3 interface{}) (arg1Data serialization.Data,
	arg2Data serialization.Data, arg3Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) || nilutil.IsNil(arg2) || nilutil.IsNil(arg3) {
		return nil, nil, nil, hzerror.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.serializationService.ToData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = p.serializationService.ToData(arg2)
	if err != nil {
		return
	}
	arg3Data, err = p.serializationService.ToData(arg3)
	return
}

func (p *proxy) validateAndSerializePredicate(arg1 interface{}) (arg1Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) {
		return nil, hzerror.NewHazelcastSerializationError(bufutil.NilPredicateIsNotAllowed, nil)
	}
	arg1Data, err = p.serializationService.ToData(arg1)
	return
}

func (p *proxy) validateAndSerializeSlice(elements []interface{}) (elementsData []serialization.Data, err error) {
	if elements == nil {
		return nil, hzerror.NewHazelcastSerializationError(bufutil.NilSliceIsNotAllowed, nil)
	}
	elementsData, err = colutil.ObjectToDataCollection(elements, p.serializationService)
	return
}

func (p *proxy) tryInvoke(ctx context.Context, f func(ctx context.Context) (interface{}, error)) (*proto.ClientMessage, error) {
	if res, err := p.circuitBreaker.TryWithContext(ctx, f).Result(); err != nil {
		return nil, err
	} else {
		return res.(*proto.ClientMessage), nil
	}
}

func (p *proxy) invokeOnKey(ctx context.Context, request *proto.ClientMessage, keyData serialization.Data) (*proto.ClientMessage, error) {
	partitionID := p.partitionService.GetPartitionID(keyData)
	return p.invokeOnPartition(ctx, request, partitionID)
}

func (p *proxy) invokeOnRandomTarget(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler) (*proto.ClientMessage, error) {
	return p.tryInvoke(ctx, func(ctx context.Context) (interface{}, error) {
		inv := p.invocationFactory.NewInvocationOnRandomTarget(request, handler)
		p.requestCh <- inv
		return inv.GetWithTimeout(1 * time.Second)
	})
}

func (p *proxy) invokeOnPartition(ctx context.Context, request *proto.ClientMessage, partitionID int32) (*proto.ClientMessage, error) {
	return p.tryInvoke(ctx, func(ctx context.Context) (interface{}, error) {
		return p.invokeOnPartitionAsync(request, partitionID).GetWithTimeout(1 * time.Second)
	})
}

func (p *proxy) invokeOnPartitionAsync(request *proto.ClientMessage, partitionID int32) invocation.Invocation {
	inv := p.invocationFactory.NewInvocationOnPartitionOwner(request, partitionID)
	p.requestCh <- inv
	return inv
}

func (p *proxy) convertToObject(data serialization.Data) (interface{}, error) {
	return p.serializationService.ToObject(data)
}

func (p *proxy) mustConvertToInterface(data serialization.Data, panicMsg string) interface{} {
	if value, err := p.serializationService.ToObject(data); err != nil {
		panic(panicMsg)
	} else {
		return value
	}
}

func (p *proxy) convertToData(object interface{}) (serialization.Data, error) {
	return p.serializationService.ToData(object)
}

func (p *proxy) decodeToObjectAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() serialization.Data) (response interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return p.convertToObject(decodeFunc(responseMessage)())
}

func (p *proxy) decodeToBoolAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() bool) (response bool, err error) {
	if inputError != nil {
		return false, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *proxy) decodeToInterfaceSliceAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() []serialization.Data) (response []interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return colutil.DataToObjectCollection(decodeFunc(responseMessage)(), p.serializationService)
}

func (p *proxy) decodeToPairSliceAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() []*proto.Pair) (response []proto.Pair, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return colutil.DataToObjectPairCollection(decodeFunc(responseMessage)(), p.serializationService)
}

func (p *proxy) decodeToInt32AndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() int32) (response int32, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *proxy) decodeToInt64AndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() int64) (response int64, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *proxy) partitionToPairs(keyValuePairs []types.Entry) (map[int32][]proto.Pair, error) {
	ps := p.partitionService
	partitionToPairs := map[int32][]proto.Pair{}
	for _, pair := range keyValuePairs {
		if keyData, valueData, err := p.validateAndSerialize2(pair.Key, pair.Value); err != nil {
			return nil, err
		} else {
			partitionKey := ps.GetPartitionID(keyData)
			arr := partitionToPairs[partitionKey]
			partitionToPairs[partitionKey] = append(arr, proto.NewPair(keyData, valueData))
		}
	}
	return partitionToPairs, nil
}

func (p *proxy) convertPairsToEntries(pairs []proto.Pair) ([]types.Entry, error) {
	kvPairs := make([]types.Entry, len(pairs))
	for i, pair := range pairs {
		key, err := p.convertToObject(pair.Key().(serialization.Data))
		if err != nil {
			return nil, err
		}
		value, err := p.convertToObject(pair.Value().(serialization.Data))
		if err != nil {
			return nil, err
		}
		kvPairs[i] = types.Entry{key, value}
	}
	return kvPairs, nil
}
