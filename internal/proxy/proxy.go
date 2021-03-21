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
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization/spi"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/colutil"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/nilutil"
)

const (
	ttlDefault   = -1
	ttlUnlimited = 0

	maxIdleDefault = -1

	threadID = 1
)

type Proxy interface {
	Destroy() error
	Smart() bool
	Name() string
	ServiceName() string
	PartitionKey() string
}

type CreationBundle struct {
	RequestCh            chan<- invocation.Invocation
	SerializationService spi.SerializationService
	PartitionService     cluster.PartitionService
	EventDispatcher      event.DispatchService
	ClusterService       cluster.Service
	InvocationFactory    invocation.Factory
	SmartRouting         bool
	ListenerBinder       proto.ListenerBinder
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
	if b.EventDispatcher == nil {
		panic("EventDispatcher is nil")
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
}

type Impl struct {
	requestCh            chan<- invocation.Invocation
	serializationService spi.SerializationService
	partitionService     cluster.PartitionService
	eventDispatcher      event.DispatchService
	clusterService       cluster.Service
	invocationFactory    invocation.Factory
	listenerBinder       proto.ListenerBinder
	smartRouting         bool
	serviceName          string
	name                 string
}

func NewImpl(bundle CreationBundle, serviceName string, objectName string) *Impl {
	bundle.Check()
	return &Impl{
		serviceName:          serviceName,
		name:                 objectName,
		requestCh:            bundle.RequestCh,
		serializationService: bundle.SerializationService,
		eventDispatcher:      bundle.EventDispatcher,
		partitionService:     bundle.PartitionService,
		clusterService:       bundle.ClusterService,
		invocationFactory:    bundle.InvocationFactory,
		listenerBinder:       bundle.ListenerBinder,
		smartRouting:         bundle.SmartRouting,
	}
}

func (p *Impl) Destroy() error {
	request := proto.ClientDestroyProxyEncodeRequest(p.name, p.serviceName)
	inv := p.invocationFactory.NewInvocationOnRandomTarget(request)
	p.requestCh <- inv
	if _, err := inv.Get(); err != nil {
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
		return nil, hzerror.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	return
}

func (p *Impl) validateAndSerialize2(arg1 interface{}, arg2 interface{}) (arg1Data serialization.Data,
	arg2Data serialization.Data, err error) {
	if nilutil.IsNil(arg1) || nilutil.IsNil(arg2) {
		return nil, nil, hzerror.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
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
		return nil, nil, nil, hzerror.NewHazelcastNilPointerError(bufutil.NilArgIsNotAllowed, nil)
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
		return nil, hzerror.NewHazelcastSerializationError(bufutil.NilPredicateIsNotAllowed, nil)
	}
	arg1Data, err = p.toData(arg1)
	return
}

func (p *Impl) validateAndSerializeSlice(elements []interface{}) (elementsData []serialization.Data, err error) {
	if elements == nil {
		return nil, hzerror.NewHazelcastSerializationError(bufutil.NilSliceIsNotAllowed, nil)
	}
	elementsData, err = colutil.ObjectToDataCollection(elements, p.serializationService)
	return
}

func (p *Impl) validateItemListener(listener interface{}) (err error) {
	/*
		switch listener.(type) {
		case core.ItemAddedListener:
		case core.ItemRemovedListener:
		default:
			return core.NewHazelcastIllegalArgumentError("listener argument type must be one of ItemAddedListener,"+
				" ItemRemovedListener", nil)
		}
		return nil
	*/
	return nil
}

func (p *Impl) validateEntryListener(listener interface{}) (err error) {
	/*
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
	*/
	return nil
}

func (p *Impl) validateAndSerializeMapAndGetPartitions(entries map[interface{}]interface{}) (map[int32][]*proto.Pair, error) {
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

func (p *Impl) invokeOnKey(request *proto.ClientMessage, keyData serialization.Data) (*proto.ClientMessage, error) {
	inv := p.invocationFactory.NewInvocationOnKeyOwner(request, keyData)
	p.requestCh <- inv
	return inv.Get()
	//select {
	//case p.requestCh <- inv:
	//	return inv.GetWithTimeout(100 * time.Millisecond)
	//case <-time.After(100 * time.Millisecond):
	//	return nil, errors.New("timeout")

	//}

}

func (p *Impl) invokeOnRandomTarget(request *proto.ClientMessage) (*proto.ClientMessage, error) {
	inv := p.invocationFactory.NewInvocationOnRandomTarget(request)
	p.requestCh <- inv
	return inv.Get()
}

func (p *Impl) invokeOnPartition(request *proto.ClientMessage, partitionID int32) (*proto.ClientMessage, error) {
	return p.invokeOnPartitionAsync(request, partitionID).Get()
}

func (p *Impl) invokeOnPartitionAsync(request *proto.ClientMessage, partitionID int32) invocation.Invocation {
	inv := p.invocationFactory.NewInvocationOnPartitionOwner(request, partitionID)
	p.requestCh <- inv
	return inv
}

func (p *Impl) invokeOnAddress(request *proto.ClientMessage, address pubcluster.Address) (*proto.ClientMessage, error) {
	inv := p.invocationFactory.NewInvocationOnTarget(request, address)
	p.requestCh <- inv
	return inv.Get()
}

func (p *Impl) toObject(data serialization.Data) (interface{}, error) {
	return p.serializationService.ToObject(data)
}

func (p *Impl) mustToInterface(data serialization.Data, panicMsg string) interface{} {
	if value, err := p.serializationService.ToObject(data); err != nil {
		panic(panicMsg)
	} else {
		return value
	}
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

/*
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
*/

type partitionSpecificProxy struct {
	*Impl
	partitionID int32
}

func newPartitionSpecificProxy(
	serializationService spi.SerializationService,
	partitionService cluster.PartitionService,
	//invocationService invocation.Service,
	requestCh chan<- invocation.Invocation,
	clusterService cluster.Service,
	smartRouting bool,
	serviceName string,
	name string,
) *partitionSpecificProxy {
	parSpecProxy := &partitionSpecificProxy{
		Impl: &Impl{
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
