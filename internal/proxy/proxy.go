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

package proxy

import (
	"context"
	"fmt"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hztypes"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization/spi"
	"github.com/hazelcast/hazelcast-go-client/internal/util/colutil"
	"github.com/hazelcast/hazelcast-go-client/internal/util/nilutil"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	ttlDefault     = -1
	ttlUnlimited   = 0
	maxIdleDefault = -1
	//threadID       = 1
)

type CreationBundle struct {
	RequestCh            chan<- invocation.Invocation
	SerializationService spi.SerializationService
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

type Proxy struct {
	requestCh            chan<- invocation.Invocation
	serializationService spi.SerializationService
	partitionService     *cluster.PartitionService
	userEventDispatcher  *event.DispatchService
	clusterService       *cluster.ServiceImpl
	invocationFactory    *cluster.ConnectionInvocationFactory
	listenerBinder       *cluster.ConnectionListenerBinderImpl
	smartRouting         bool
	serviceName          string
	name                 string
	logger               logger.Logger
	cb                   *cb.CircuitBreaker
}

func NewProxy(bundle CreationBundle, serviceName string, objectName string) *Proxy {
	bundle.Check()
	// TODO: make circuit breaker configurable
	circuitBreaker := cb.NewCircuitBreaker(
		cb.MaxRetries(10),
		cb.MaxFailureCount(10),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration((attempt+1)*200) * time.Millisecond
		}))
	return &Proxy{
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
		cb:                   circuitBreaker,
	}
}

func (p *Proxy) Destroy() error {
	request := proto.ClientDestroyProxyEncodeRequest(p.name, p.serviceName)
	inv := p.invocationFactory.NewInvocationOnRandomTarget(request, nil)
	p.requestCh <- inv
	if _, err := inv.Get(); err != nil {
		return fmt.Errorf("error destroying proxy: %w", err)
	}
	return nil
}

func (p Proxy) Smart() bool {
	return p.smartRouting
}

func (p *Proxy) Name() string {
	return p.name
}

func (p *Proxy) PartitionKey() string {
	return p.name
}

func (p *Proxy) ServiceName() string {
	return p.serviceName
}

func (p *Proxy) validateAndSerialize(arg1 interface{}) (arg1Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) {
		return nil, hzerror.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.serializationService.ToData(arg1)
	return
}

func (p *Proxy) validateAndSerialize2(arg1 interface{}, arg2 interface{}) (arg1Data serialization.Data,
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

func (p *Proxy) validateAndSerialize3(arg1 interface{}, arg2 interface{}, arg3 interface{}) (arg1Data serialization.Data,
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

func (p *Proxy) validateAndSerializePredicate(arg1 interface{}) (arg1Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) {
		return nil, hzerror.NewHazelcastSerializationError(bufutil.NilPredicateIsNotAllowed, nil)
	}
	arg1Data, err = p.serializationService.ToData(arg1)
	return
}

func (p *Proxy) validateAndSerializeSlice(elements []interface{}) (elementsData []serialization.Data, err error) {
	if elements == nil {
		return nil, hzerror.NewHazelcastSerializationError(bufutil.NilSliceIsNotAllowed, nil)
	}
	elementsData, err = colutil.ObjectToDataCollection(elements, p.serializationService)
	return
}

func (p *Proxy) validateAndSerializeMapAndGetPartitions(entries map[interface{}]interface{}) (map[int32][]*proto.Pair, error) {
	if entries == nil {
		return nil, hzerror.NewHazelcastNilPointerError(bufutil.NilMapIsNotAllowed, nil)
	}
	partitions := make(map[int32][]*proto.Pair)
	for key, value := range entries {
		keyData, valueData, err := p.validateAndSerialize2(key, value)
		if err != nil {
			return nil, err
		}
		pair := proto.NewPair(keyData, valueData)
		partitionID := p.partitionService.GetPartitionID(keyData)
		partitions[partitionID] = append(partitions[partitionID], &pair)
	}
	return partitions, nil
}

func (p *Proxy) tryInvoke(f func(ctx context.Context) (interface{}, error)) (*proto.ClientMessage, error) {
	if res, err := p.cb.Try(f).Result(); err != nil {
		return nil, err
	} else {
		return res.(*proto.ClientMessage), nil
	}
}

func (p *Proxy) invokeOnKey(request *proto.ClientMessage, keyData serialization.Data) (*proto.ClientMessage, error) {
	partitionID := p.partitionService.GetPartitionID(keyData)
	return p.invokeOnPartition(request, partitionID)
}

func (p *Proxy) invokeOnRandomTarget(request *proto.ClientMessage, handler proto.ClientMessageHandler) (*proto.ClientMessage, error) {
	return p.tryInvoke(func(ctx context.Context) (interface{}, error) {
		inv := p.invocationFactory.NewInvocationOnRandomTarget(request, handler)
		p.requestCh <- inv
		return inv.GetWithTimeout(1 * time.Second)
	})
}

func (p *Proxy) invokeOnPartition(request *proto.ClientMessage, partitionID int32) (*proto.ClientMessage, error) {
	return p.tryInvoke(func(ctx context.Context) (interface{}, error) {
		return p.invokeOnPartitionAsync(request, partitionID).GetWithTimeout(1 * time.Second)
	})
}

func (p *Proxy) invokeOnPartitionAsync(request *proto.ClientMessage, partitionID int32) invocation.Invocation {
	inv := p.invocationFactory.NewInvocationOnPartitionOwner(request, partitionID)
	p.requestCh <- inv
	return inv
}

func (p *Proxy) invokeOnAddress(request *proto.ClientMessage, address *pubcluster.AddressImpl) (*proto.ClientMessage, error) {
	return p.tryInvoke(func(ctx context.Context) (interface{}, error) {
		inv := p.invocationFactory.NewInvocationOnTarget(request, address)
		p.requestCh <- inv
		return inv.GetWithTimeout(1 * time.Second)
	})
}

func (p *Proxy) convertToObject(data serialization.Data) (interface{}, error) {
	return p.serializationService.ToObject(data)
}

func (p *Proxy) mustConvertToInterface(data serialization.Data, panicMsg string) interface{} {
	if value, err := p.serializationService.ToObject(data); err != nil {
		panic(panicMsg)
	} else {
		return value
	}
}

func (p *Proxy) convertToData(object interface{}) (serialization.Data, error) {
	return p.serializationService.ToData(object)
}

func (p *Proxy) decodeToObjectAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() serialization.Data) (response interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return p.convertToObject(decodeFunc(responseMessage)())
}

func (p *Proxy) decodeToBoolAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() bool) (response bool, err error) {
	if inputError != nil {
		return false, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *Proxy) decodeToInterfaceSliceAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() []serialization.Data) (response []interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return colutil.DataToObjectCollection(decodeFunc(responseMessage)(), p.serializationService)
}

func (p *Proxy) decodeToPairSliceAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() []*proto.Pair) (response []proto.Pair, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return colutil.DataToObjectPairCollection(decodeFunc(responseMessage)(), p.serializationService)
}

func (p *Proxy) decodeToInt32AndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() int32) (response int32, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *Proxy) decodeToInt64AndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() int64) (response int64, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *Proxy) partitionToPairs(keyValuePairs []hztypes.Entry) (map[int32][]proto.Pair, error) {
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

func (p *Proxy) convertPairsToEntries(pairs []proto.Pair) ([]hztypes.Entry, error) {
	kvPairs := make([]hztypes.Entry, len(pairs))
	for i, pair := range pairs {
		key, err := p.convertToObject(pair.Key().(serialization.Data))
		if err != nil {
			return nil, err
		}
		value, err := p.convertToObject(pair.Value().(serialization.Data))
		if err != nil {
			return nil, err
		}
		kvPairs[i] = hztypes.Entry{key, value}
	}
	return kvPairs, nil
}

type partitionSpecificProxy struct {
	*Proxy
	partitionID int32
}

func newPartitionSpecificProxy(
	serializationService spi.SerializationService,
	partitionService *cluster.PartitionService,
	//invocationService invocation.Service,
	requestCh chan<- invocation.Invocation,
	clusterService *cluster.ServiceImpl,
	smartRouting bool,
	serviceName string,
	name string,
) *partitionSpecificProxy {
	parSpecProxy := &partitionSpecificProxy{
		Proxy: &Proxy{
			requestCh:            requestCh,
			serializationService: serializationService,
			partitionService:     partitionService,
			clusterService:       clusterService,
			smartRouting:         smartRouting,
			serviceName:          serviceName,
			name:                 name,
		},
	}
	var err error
	if parSpecProxy.partitionID, err = partitionService.GetPartitionIDWithKey(parSpecProxy.PartitionKey()); err != nil {
		panic(fmt.Errorf("error creating partitionSpecificProxy: %w", err))
	} else {
		return parSpecProxy
	}
}

func (parSpecProxy *partitionSpecificProxy) invoke(request *proto.ClientMessage) (*proto.ClientMessage, error) {
	return parSpecProxy.invokeOnPartition(request, parSpecProxy.partitionID)
}
