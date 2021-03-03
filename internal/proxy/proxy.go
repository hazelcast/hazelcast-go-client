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
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization/spi"
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/colutil"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/nilutil"
)

const (
	threadID     = 1
	ttlUnlimited = 0
)

type Proxy interface {
	Destroy() error
	Smart() bool
	Name() string
	ServiceName() string
	PartitionKey() string
}

type ProxyCreationBundle struct {
	SerializationService spi.SerializationService
	PartitionService     cluster.PartitionService
	InvocationService    invocation.Service
	ClusterService       cluster.Service
	InvocationFactory    invocation.Factory
	SmartRouting         bool
}

func (b ProxyCreationBundle) Check() {
	if b.SerializationService == nil {
		panic("SerializationService is nil")
	}
	if b.PartitionService == nil {
		panic("PartitionService is nil")
	}
	if b.InvocationService == nil {
		panic("InvocationService is nil")
	}
	if b.ClusterService == nil {
		panic("ClusterService is nil")
	}
	if b.InvocationFactory == nil {
		panic("ConnectionInvocationFactory is nil")
	}
}

type Impl struct {
	//client               *HazelcastClient
	//proxyManager         Manager
	serializationService spi.SerializationService
	partitionService     cluster.PartitionService
	invocationService    invocation.Service
	clusterService       cluster.Service
	invocationFactory    invocation.Factory
	smartRouting         bool
	serviceName          string
	name                 string
}

func NewImpl(bundle ProxyCreationBundle, serviceName string, objectName string) *Impl {
	bundle.Check()
	return &Impl{
		serviceName:          serviceName,
		name:                 objectName,
		serializationService: bundle.SerializationService,
		invocationService:    bundle.InvocationService,
		partitionService:     bundle.PartitionService,
		clusterService:       bundle.ClusterService,
		invocationFactory:    bundle.InvocationFactory,
		smartRouting:         bundle.SmartRouting,
	}
}

func (p *Impl) Destroy() error {
	request := proto.ClientDestroyProxyEncodeRequest(p.name, p.serviceName)
	inv := p.invocationFactory.NewInvocationOnRandomTarget(request)
	if _, err := p.invocationService.Send(inv).Get(); err != nil {
		return fmt.Errorf("error destroying proxy: %w", err)
	}
	return nil
}

func (p Impl) Smart() bool {
	return p.smartRouting
}

func (p *Impl) Name() string {
	return p.name
}

func (p *Impl) PartitionKey() string {
	return p.name
}

func (p *Impl) ServiceName() string {
	return p.serviceName
}

func (p *Impl) validateAndSerialize(arg1 interface{}) (arg1Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) {
		return nil, core.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	return
}

func (p *Impl) validateAndSerialize2(arg1 interface{}, arg2 interface{}) (arg1Data serialization.Data,
	arg2Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) || nilutil.IsNil(arg2) {
		return nil, nil, core.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = p.toData(arg2)
	return
}

func (p *Impl) validateAndSerialize3(arg1 interface{}, arg2 interface{}, arg3 interface{}) (arg1Data serialization.Data,
	arg2Data serialization.Data, arg3Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) || nilutil.IsNil(arg2) || nilutil.IsNil(arg3) {
		return nil, nil, nil, core.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	if err != nil {
		return
	}
	arg2Data, err = p.toData(arg2)
	if err != nil {
		return
	}
	arg3Data, err = p.toData(arg3)
	return
}

func (p *Impl) validateAndSerializePredicate(arg1 interface{}) (arg1Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) {
		return nil, core.NewHazelcastSerializationError(bufutil.NilPredicateIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	return
}

func (p *Impl) validateAndSerializeSlice(elements []interface{}) (elementsData []serialization.Data, err error) {
	if elements == nil {
		return nil, core.NewHazelcastSerializationError(bufutil.NilSliceIsNotAllowed, nil)
	}
	elementsData, err = colutil.ObjectToDataCollection(elements, p.serializationService)
	return
}

func (p *Impl) validateItemListener(listener interface{}) (err error) {
	switch listener.(type) {
	case core.ItemAddedListener:
	case core.ItemRemovedListener:
	default:
		return core.NewHazelcastIllegalArgumentError("listener argument type must be one of ItemAddedListener,"+
			" ItemRemovedListener", nil)
	}
	return nil
}

func (p *Impl) validateEntryListener(listener interface{}) (err error) {
	argErr := core.NewHazelcastIllegalArgumentError(fmt.Sprintf("not a supported listener type: %v",
		reflect.TypeOf(listener)), nil)
	if p.serviceName == bufutil.ServiceNameReplicatedMap {
		switch listener.(type) {
		case core.EntryAddedListener:
		case core.EntryRemovedListener:
		case core.EntryUpdatedListener:
		case core.EntryEvictedListener:
		case core.MapClearedListener:
		default:
			err = argErr
		}
	} else if p.serviceName == bufutil.ServiceNameMultiMap {
		switch listener.(type) {
		case core.EntryAddedListener:
		case core.EntryRemovedListener:
		case core.MapClearedListener:
		default:
			err = argErr
		}
	}
	return
}

func (p *Impl) validateAndSerializeMapAndGetPartitions(entries map[interface{}]interface{}) (map[int32][]*proto.Pair, error) {
	if entries == nil {
		return nil, core.NewHazelcastNilPointerError(bufutil.NilMapIsNotAllowed, nil)
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

func (p *Impl) invokeOnKey(request *proto.ClientMessage, keyData serialization.Data) (*proto.ClientMessage, error) {
	inv := p.invocationFactory.NewInvocationOnKeyOwner(request, keyData)
	return p.invocationService.Send(inv).Get()
}

func (p *Impl) invokeOnRandomTarget(request *proto.ClientMessage) (*proto.ClientMessage, error) {
	inv := p.invocationFactory.NewInvocationOnRandomTarget(request)
	return p.invocationService.Send(inv).Get()
}

func (p *Impl) invokeOnPartition(request *proto.ClientMessage, partitionID int32) (*proto.ClientMessage, error) {
	inv := p.invocationFactory.NewInvocationOnPartitionOwner(request, partitionID)
	return p.invocationService.Send(inv).Get()
}

func (p *Impl) invokeOnAddress(request *proto.ClientMessage, address *core.Address) (*proto.ClientMessage, error) {
	inv := p.invocationFactory.NewInvocationOnTarget(request, address)
	return p.invocationService.Send(inv).Get()
}

func (p *Impl) toObject(data serialization.Data) (interface{}, error) {
	return p.serializationService.ToObject(data)
}

func (p *Impl) toData(object interface{}) (serialization.Data, error) {
	return p.serializationService.ToData(object)
}

func (p *Impl) decodeToObjectAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() serialization.Data) (response interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return p.toObject(decodeFunc(responseMessage)())
}

func (p *Impl) decodeToBoolAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() bool) (response bool, err error) {
	if inputError != nil {
		return false, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *Impl) decodeToInterfaceSliceAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() []serialization.Data) (response []interface{}, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return colutil.DataToObjectCollection(decodeFunc(responseMessage)(), p.serializationService)
}

func (p *Impl) decodeToPairSliceAndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() []*proto.Pair) (response []proto.Pair, err error) {
	if inputError != nil {
		return nil, inputError
	}
	return colutil.DataToObjectPairCollection(decodeFunc(responseMessage)(), p.serializationService)
}

func (p *Impl) decodeToInt32AndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() int32) (response int32, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *Impl) decodeToInt64AndError(responseMessage *proto.ClientMessage, inputError error,
	decodeFunc func(*proto.ClientMessage) func() int64) (response int64, err error) {
	if inputError != nil {
		return 0, inputError
	}
	return decodeFunc(responseMessage)(), nil
}

func (p *Impl) createOnItemEvent(listener interface{}) func(itemData serialization.Data, uuid string, eventType int32) {
	return func(itemData serialization.Data, uuid string, eventType int32) {
		var item interface{}
		item, _ = p.toObject(itemData)
		member := p.clusterService.GetMemberByUUID(uuid)
		itemEvent := proto.NewItemEvent(p.name, item, eventType, member.(*proto.Member))
		if eventType == bufutil.ItemAdded {
			if _, ok := listener.(core.ItemAddedListener); ok {
				listener.(core.ItemAddedListener).ItemAdded(itemEvent)
			}
		} else if eventType == bufutil.ItemRemoved {
			if _, ok := listener.(core.ItemRemovedListener); ok {
				listener.(core.ItemRemovedListener).ItemRemoved(itemEvent)
			}
		}
	}
}

type partitionSpecificProxy struct {
	*Impl
	partitionID int32
}

func newPartitionSpecificProxy(
	serializationService spi.SerializationService,
	partitionService cluster.PartitionService,
	invocationService invocation.Service,
	clusterService cluster.Service,
	smartRouting bool,
	serviceName string,
	name string,
) *partitionSpecificProxy {
	parSpecProxy := &partitionSpecificProxy{
		Impl: &Impl{
			serializationService: serializationService,
			partitionService:     partitionService,
			invocationService:    invocationService,
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
