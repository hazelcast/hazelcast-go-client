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
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/hazelcast/hazelcast-go-client/aggregate"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/util/nilutil"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	ServiceNameMap              = "hz:impl:mapService"
	ServiceNameReplicatedMap    = "hz:impl:replicatedMapService"
	ServiceNameQueue            = "hz:impl:queueService"
	ServiceNameTopic            = "hz:impl:topicService"
	ServiceNameList             = "hz:impl:listService"
	ServiceNameSet              = "hz:impl:setService"
	ServiceNamePNCounter        = "hz:impl:PNCounterService"
	ServiceNameFlakeIDGenerator = "hz:impl:flakeIdGeneratorService"
)

const (
	ttlUnset     = -1
	ttlUnlimited = 0
)

type creationBundle struct {
	RequestCh            chan<- invocation.Invocation
	RemoveCh             chan<- int64
	SerializationService *iserialization.Service
	PartitionService     *cluster.PartitionService
	ClusterService       *cluster.Service
	InvocationFactory    *cluster.ConnectionInvocationFactory
	ListenerBinder       *cluster.ConnectionListenerBinder
	Config               *Config
	Logger               ilogger.Logger
}

func (b creationBundle) Check() {
	if b.RequestCh == nil {
		panic("RequestCh is nil")
	}
	if b.RemoveCh == nil {
		panic("RemoveCh is nil")
	}
	if b.SerializationService == nil {
		panic("SerializationService is nil")
	}
	if b.PartitionService == nil {
		panic("PartitionService is nil")
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
	if b.Config == nil {
		panic("Config is nil")
	}
	if b.Logger == nil {
		panic("Logger is nil")
	}
}

type proxy struct {
	logger               ilogger.Logger
	requestCh            chan<- invocation.Invocation
	removeCh             chan<- int64
	serializationService *iserialization.Service
	partitionService     *cluster.PartitionService
	listenerBinder       *cluster.ConnectionListenerBinder
	config               *Config
	clusterService       *cluster.Service
	invocationFactory    *cluster.ConnectionInvocationFactory
	cb                   *cb.CircuitBreaker
	refIDGen             *iproxy.ReferenceIDGenerator
	removeFromCacheFn    func() bool
	serviceName          string
	name                 string
	smart                bool
}

func newProxy(
	ctx context.Context,
	bundle creationBundle,
	serviceName string,
	objectName string,
	refIDGen *iproxy.ReferenceIDGenerator,
	removeFromCacheFn func() bool,
	remote bool) (*proxy, error) {

	bundle.Check()
	// TODO: make circuit breaker configurable
	circuitBreaker := cb.NewCircuitBreaker(
		cb.MaxRetries(math.MaxInt64),
		cb.MaxFailureCount(10),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration((attempt+1)*100) * time.Millisecond
		}))
	p := &proxy{
		serviceName:          serviceName,
		name:                 objectName,
		requestCh:            bundle.RequestCh,
		removeCh:             bundle.RemoveCh,
		serializationService: bundle.SerializationService,
		partitionService:     bundle.PartitionService,
		clusterService:       bundle.ClusterService,
		invocationFactory:    bundle.InvocationFactory,
		listenerBinder:       bundle.ListenerBinder,
		config:               bundle.Config,
		logger:               bundle.Logger,
		cb:                   circuitBreaker,
		removeFromCacheFn:    removeFromCacheFn,
		refIDGen:             refIDGen,
		smart:                !bundle.Config.Cluster.Unisocket,
	}
	if !remote {
		return p, nil
	}
	if err := p.create(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *proxy) create(ctx context.Context) error {
	request := codec.EncodeClientCreateProxyRequest(p.name, p.serviceName)
	if _, err := p.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return fmt.Errorf("error creating proxy: %w", err)
	}
	return nil
}

// Destroy removes this object cluster-wide.
// Clears and releases all resources for this object.
func (p *proxy) Destroy(ctx context.Context) error {
	// wipe from proxy manager cache
	if !p.removeFromCacheFn() {
		// no need to destroy on cluster, since the proxy is stale and was already destroyed
		return nil
	}
	// destroy on cluster
	request := codec.EncodeClientDestroyProxyRequest(p.name, p.serviceName)
	if _, err := p.invokeOnRandomTarget(ctx, request, nil); err != nil {
		return fmt.Errorf("error destroying proxy: %w", err)
	}
	return nil
}

func (p *proxy) validateAndSerialize(arg1 interface{}) (*iserialization.Data, error) {
	if nilutil.IsNil(arg1) {
		return nil, ihzerrors.NewIllegalArgumentError("nil arg is not allowed", nil)
	}
	return p.serializationService.ToData(arg1)
}

func (p *proxy) validateAndSerialize2(arg1 interface{}, arg2 interface{}) (arg1Data *iserialization.Data,
	arg2Data *iserialization.Data, err error) {
	if nilutil.IsNil(arg1) || nilutil.IsNil(arg2) {
		return nil, nil, ihzerrors.NewIllegalArgumentError("nil arg is not allowed", nil)
	}
	arg1Data, err = p.serializationService.ToData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = p.serializationService.ToData(arg2)
	return
}

func (p *proxy) validateAndSerialize3(arg1 interface{}, arg2 interface{}, arg3 interface{}) (arg1Data *iserialization.Data,
	arg2Data *iserialization.Data, arg3Data *iserialization.Data, err error) {
	if nilutil.IsNil(arg1) || nilutil.IsNil(arg2) || nilutil.IsNil(arg3) {
		return nil, nil, nil, ihzerrors.NewIllegalArgumentError("nil arg is not allowed", nil)
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

func (p *proxy) validateAndSerializeAggregate(agg aggregate.Aggregator) (arg1Data *iserialization.Data, err error) {
	if nilutil.IsNil(agg) {
		return nil, ihzerrors.NewIllegalArgumentError("aggregate should not be nil", nil)
	}
	arg1Data, err = p.serializationService.ToData(agg)
	return
}

func (p *proxy) validateAndSerializePredicate(pred predicate.Predicate) (arg1Data *iserialization.Data, err error) {
	if nilutil.IsNil(pred) {
		return nil, ihzerrors.NewIllegalArgumentError("predicate should not be nil", nil)
	}
	arg1Data, err = p.serializationService.ToData(pred)
	return
}

func (p *proxy) validateAndSerializeValues(values []interface{}) ([]*iserialization.Data, error) {
	valuesData := make([]*iserialization.Data, len(values))
	for i, value := range values {
		if data, err := p.validateAndSerialize(value); err != nil {
			return nil, err
		} else {
			valuesData[i] = data
		}
	}
	return valuesData, nil
}

func (p *proxy) tryInvoke(ctx context.Context, f cb.TryHandler) (*proto.ClientMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if res, err := p.cb.TryContext(ctx, f); err != nil {
		return nil, err
	} else {
		return res.(*proto.ClientMessage), nil
	}
}

func (p *proxy) invokeOnKey(ctx context.Context, request *proto.ClientMessage, keyData *iserialization.Data) (*proto.ClientMessage, error) {
	if partitionID, err := p.partitionService.GetPartitionID(keyData); err != nil {
		return nil, err
	} else {
		return p.invokeOnPartition(ctx, request, partitionID)
	}
}

func (p *proxy) invokeOnRandomTarget(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler) (*proto.ClientMessage, error) {
	return p.tryInvoke(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt > 0 {
			request = request.Copy()
		}
		inv := p.invocationFactory.NewInvocationOnRandomTarget(request, handler)
		if err := p.sendInvocation(ctx, inv); err != nil {
			return nil, err
		}
		return inv.GetWithContext(ctx)
	})
}

func (p *proxy) invokeOnPartition(ctx context.Context, request *proto.ClientMessage, partitionID int32) (*proto.ClientMessage, error) {
	return p.tryInvoke(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if inv, err := p.invokeOnPartitionAsync(ctx, request, partitionID); err != nil {
			return nil, err
		} else {
			return inv.GetWithContext(ctx)
		}
	})
}

func (p *proxy) invokeOnPartitionAsync(ctx context.Context, request *proto.ClientMessage, partitionID int32) (invocation.Invocation, error) {
	inv := p.invocationFactory.NewInvocationOnPartitionOwner(request, partitionID)
	err := p.sendInvocation(ctx, inv)
	return inv, err
}

func (p *proxy) convertToObject(data *iserialization.Data) (interface{}, error) {
	return p.serializationService.ToObject(data)
}

func (p *proxy) convertToObjects(values []*iserialization.Data) ([]interface{}, error) {
	decodedValues := make([]interface{}, len(values))
	for i, value := range values {
		if decodedValue, err := p.convertToObject(value); err != nil {
			return nil, err
		} else {
			decodedValues[i] = decodedValue
		}
	}
	return decodedValues, nil
}

func (p *proxy) convertToData(object interface{}) (*iserialization.Data, error) {
	return p.serializationService.ToData(object)
}

func (p *proxy) partitionToPairs(keyValuePairs []types.Entry) (map[int32][]proto.Pair, error) {
	ps := p.partitionService
	partitionToPairs := map[int32][]proto.Pair{}
	for _, pair := range keyValuePairs {
		if keyData, valueData, err := p.validateAndSerialize2(pair.Key, pair.Value); err != nil {
			return nil, err
		} else {
			if partitionKey, err := ps.GetPartitionID(keyData); err != nil {
				return nil, err
			} else {
				arr := partitionToPairs[partitionKey]
				partitionToPairs[partitionKey] = append(arr, proto.NewPair(keyData, valueData))
			}
		}
	}
	return partitionToPairs, nil
}

func (p *proxy) convertPairsToEntries(pairs []proto.Pair) ([]types.Entry, error) {
	kvPairs := make([]types.Entry, len(pairs))
	for i, pair := range pairs {
		key, err := p.convertToObject(pair.Key().(*iserialization.Data))
		if err != nil {
			return nil, err
		}
		value, err := p.convertToObject(pair.Value().(*iserialization.Data))
		if err != nil {
			return nil, err
		}
		kvPairs[i] = types.Entry{Key: key, Value: value}
	}
	return kvPairs, nil
}

func (p *proxy) putAll(keyValuePairs []types.Entry, f func(partitionID int32, entries []proto.Pair) cb.Future) error {
	if partitionToPairs, err := p.partitionToPairs(keyValuePairs); err != nil {
		return err
	} else {
		// create futures
		futures := make([]cb.Future, 0, len(partitionToPairs))
		for partitionID, entries := range partitionToPairs {
			futures = append(futures, f(partitionID, entries))
		}
		for _, future := range futures {
			if _, err := future.Result(); err != nil {
				return err
			}
		}
		return nil
	}
}

func (p *proxy) stringToPartitionID(key string) (int32, error) {
	idx := strings.Index(key, "@")
	if keyData, err := p.convertToData(key[idx+1:]); err != nil {
		return 0, err
	} else if partitionID, err := p.partitionService.GetPartitionID(keyData); err != nil {
		return 0, err
	} else {
		return partitionID, nil
	}
}

func (p *proxy) decodeEntryNotified(binKey, binValue, binOldValue, binMergingValue *iserialization.Data) (key, value, oldValue, mergingValue interface{}, err error) {
	if key, err = p.convertToObject(binKey); err != nil {
		err = fmt.Errorf("invalid key: %w", err)
	} else if value, err = p.convertToObject(binValue); err != nil {
		err = fmt.Errorf("invalid value: %w", err)
	} else if oldValue, err = p.convertToObject(binOldValue); err != nil {
		err = fmt.Errorf("invalid oldValue: %w", err)
	} else if mergingValue, err = p.convertToObject(binMergingValue); err != nil {
		err = fmt.Errorf("invalid mergingValue: %w", err)
	}
	return
}

func (p *proxy) makeEntryNotifiedListenerHandler(handler EntryNotifiedHandler) entryNotifiedHandler {
	return func(
		binKey, binValue, binOldValue, binMergingValue *iserialization.Data,
		binEventType int32,
		binUUID types.UUID,
		affectedEntries int32) {
		key, value, oldValue, mergingValue, err := p.decodeEntryNotified(binKey, binValue, binOldValue, binMergingValue)
		if err != nil {
			p.logger.Errorf("error at AddEntryListener: %w", err)
			return
		}
		member := p.clusterService.GetMemberByUUID(binUUID)
		handler(newEntryNotifiedEvent(p.name, *member, key, value, oldValue, mergingValue, int(affectedEntries), EntryEventType(binEventType)))
	}
}

func (p *proxy) sendInvocation(ctx context.Context, inv invocation.Invocation) error {
	select {
	case p.requestCh <- inv:
		return nil
	case <-ctx.Done():
		p.removeCh <- inv.Request().CorrelationID()
		return ctx.Err()
	}
}

type entryNotifiedHandler func(
	binKey, binValue, binOldValue, binMergingValue *iserialization.Data,
	binEventType int32,
	binUUID types.UUID,
	affectedEntries int32)
