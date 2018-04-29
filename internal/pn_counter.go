// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package internal

import (
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

type pnCounterProxy struct {
	*proxy
	targetSelectionMutex        sync.RWMutex // guards currentTargetReplicaAddress
	currentTargetReplicaAddress core.IAddress
	maxConfiguredReplicaCount   int32
	emptyAddresses              map[core.IAddress]struct{}
	observedClock               unsafe.Pointer
	random                      *rand.Rand
}

func newPNCounterProxy(client *HazelcastClient, serviceName *string, name *string) (*pnCounterProxy, error) {
	pn := &pnCounterProxy{
		proxy:          &proxy{client, serviceName, name},
		emptyAddresses: make(map[core.IAddress]struct{}),
	}
	atomic.StorePointer(&pn.observedClock, unsafe.Pointer(newVectorClock()))
	pn.random = rand.New(rand.NewSource(time.Now().UnixNano()))
	return pn, nil
}

func (pn *pnCounterProxy) Get() (currentValue int64, err error) {
	target, err := pn.getCRDTOperationTarget(pn.emptyAddresses)
	if err != nil {
		return
	}
	if target == nil {
		err = core.NewHazelcastNoDataMemberInClusterError("cannot invoke operations on a CRDT"+
			" because the cluster does not contain any data members", nil)
		return
	}
	response, err := pn.invokeGetInternal(pn.emptyAddresses, nil, target)
	if err != nil {
		return
	}
	value, replicaTimestamps, _ := protocol.PNCounterGetDecodeResponse(response)()
	pn.updateObservedReplicaTimestamps(replicaTimestamps)
	return value, nil
}

func (pn *pnCounterProxy) operation(delta int64, getBeforeUpdate bool) (value int64, err error) {
	target, err := pn.getCRDTOperationTarget(pn.emptyAddresses)
	if err != nil {
		return
	}
	if target == nil {
		err = core.NewHazelcastNoDataMemberInClusterError("cannot invoke operations on a CRDT"+
			" because the cluster does not contain any data members", nil)
		return
	}
	response, err := pn.invokeAddInternal(delta, getBeforeUpdate, pn.emptyAddresses, nil, target)
	if err != nil {
		return
	}
	value, replicaTimestamps, _ := protocol.PNCounterAddDecodeResponse(response)()
	pn.updateObservedReplicaTimestamps(replicaTimestamps)
	return value, nil
}

func (pn *pnCounterProxy) GetAndAdd(delta int64) (previousValue int64, err error) {
	return pn.operation(delta, true)
}

func (pn *pnCounterProxy) AddAndGet(delta int64) (updatedValue int64, err error) {
	return pn.operation(delta, false)
}

func (pn *pnCounterProxy) GetAndSubtract(delta int64) (previousValue int64, err error) {
	return pn.operation(-delta, true)
}

func (pn *pnCounterProxy) SubtractAndGet(delta int64) (updatedValue int64, err error) {
	return pn.operation(-delta, false)
}

func (pn *pnCounterProxy) DecrementAndGet() (updatedValue int64, err error) {
	return pn.operation(-1, false)
}

func (pn *pnCounterProxy) IncrementAndGet() (updatedValue int64, err error) {
	return pn.operation(1, false)
}

func (pn *pnCounterProxy) GetAndDecrement() (previousValue int64, err error) {
	return pn.operation(-1, true)
}

func (pn *pnCounterProxy) GetAndIncrement() (previousValue int64, err error) {
	return pn.operation(1, true)
}

func (pn *pnCounterProxy) Reset() {
	atomic.StorePointer(&pn.observedClock, unsafe.Pointer(newVectorClock()))
}

func (pn *pnCounterProxy) getCRDTOperationTarget(excludedAddresses map[core.IAddress]struct{}) (core.IAddress, error) {
	pn.targetSelectionMutex.RLock()
	localCurrentTargetReplicaAddress := pn.currentTargetReplicaAddress
	pn.targetSelectionMutex.RUnlock()
	_, isExcluded := excludedAddresses[localCurrentTargetReplicaAddress]
	if localCurrentTargetReplicaAddress != nil && !isExcluded {
		return localCurrentTargetReplicaAddress, nil
	}
	var err error
	_, isExcluded = excludedAddresses[localCurrentTargetReplicaAddress]
	if localCurrentTargetReplicaAddress == nil || isExcluded {
		pn.targetSelectionMutex.Lock()
		pn.currentTargetReplicaAddress, err = pn.chooseTargetReplica(excludedAddresses)
		localCurrentTargetReplicaAddress = pn.currentTargetReplicaAddress
		pn.targetSelectionMutex.Unlock()
		if err != nil {
			return nil, err
		}
	}
	return localCurrentTargetReplicaAddress, nil
}

func (pn *pnCounterProxy) invokeGetInternal(excludedAddresses map[core.IAddress]struct{}, lastError error,
	target core.IAddress) (response *protocol.ClientMessage, err error) {
	if target == nil {
		if lastError != nil {
			err = lastError
		} else {
			err = core.NewHazelcastNoDataMemberInClusterError("cannot invoke operations on a CRDT"+
				" because the cluster does not contain any data members", nil)
		}
		return
	}
	request := protocol.PNCounterGetEncodeRequest(pn.name,
		(*vectorClock)(atomic.LoadPointer(&pn.observedClock)).EntrySet(), target.(*protocol.Address))
	response, lastError = pn.invokeOnAddress(request, target.(*protocol.Address))
	if lastError != nil {
		log.Printf("error occurred while invoking operation on target %v, choosing different target", target)
		excludedAddresses[target] = struct{}{}
		newTarget, err := pn.getCRDTOperationTarget(excludedAddresses)
		if err != nil {
			return nil, err
		}
		return pn.invokeGetInternal(excludedAddresses, lastError, newTarget)
	}
	return response, nil

}

func (pn *pnCounterProxy) invokeAddInternal(delta int64, getBeforeUpdate bool,
	excludedAddresses map[core.IAddress]struct{}, lastError error,
	target core.IAddress) (response *protocol.ClientMessage, err error) {
	if target == nil {
		if lastError != nil {
			err = lastError
		} else {
			err = core.NewHazelcastNoDataMemberInClusterError("cannot invoke operations on a CRDT"+
				" because the cluster does not contain any data members", nil)
		}
		return
	}
	request := protocol.PNCounterAddEncodeRequest(pn.name, delta, getBeforeUpdate,
		(*vectorClock)(atomic.LoadPointer(&pn.observedClock)).EntrySet(), target.(*protocol.Address))
	response, lastError = pn.invokeOnAddress(request, target.(*protocol.Address))
	if lastError != nil {
		log.Printf("unable to provide session guarantees when sending operations to %v, choosing different target", target)
		excludedAddresses[target] = struct{}{}
		newTarget, err := pn.getCRDTOperationTarget(excludedAddresses)
		if err != nil {
			return nil, err
		}
		return pn.invokeAddInternal(delta, getBeforeUpdate, excludedAddresses, lastError, newTarget)
	}
	return response, nil
}

func (pn *pnCounterProxy) chooseTargetReplica(excludedAddresses map[core.IAddress]struct{}) (core.IAddress, error) {
	replicaAddresses, err := pn.getReplicaAddresses(excludedAddresses)
	if err != nil || len(replicaAddresses) == 0 {
		return nil, err
	}
	return replicaAddresses[pn.random.Intn(len(replicaAddresses))], nil
}

func (pn *pnCounterProxy) getReplicaAddresses(excludedAddresses map[core.IAddress]struct{}) ([]core.IAddress, error) {
	dataMembers := pn.client.ClusterService.MembersWithSelector(core.MemberSelectors.DataMemberSelector)
	maxConfiguredReplicaCount, err := pn.getMaxConfiguredReplicaCount()
	if err != nil {
		return nil, err
	}
	currentReplicaCount := int(math.Min(float64(maxConfiguredReplicaCount), float64(len(dataMembers))))
	var replicaAdresses []core.IAddress
	for i := 0; i < currentReplicaCount; i++ {
		dataMemberAddress := dataMembers[i].Address()
		_, ok := excludedAddresses[dataMemberAddress]
		if !ok {
			replicaAdresses = append(replicaAdresses, dataMemberAddress)
		}
	}
	return replicaAdresses, nil
}

// GetCurrentTargetReplicaAddress returns the current target replica address to which this proxy is
// sending invocations.
// It is public for testing purposes.
func GetCurrentTargetReplicaAddress(pn core.PNCounter) core.IAddress {
	return pn.(*pnCounterProxy).currentTargetReplicaAddress
}

func (pn *pnCounterProxy) getMaxConfiguredReplicaCount() (int32, error) {
	if pn.maxConfiguredReplicaCount > 0 {
		return pn.maxConfiguredReplicaCount, nil
	}
	request := protocol.PNCounterGetConfiguredReplicaCountEncodeRequest(pn.name)
	response, err := pn.invokeOnRandomTarget(request)
	if err != nil {
		return 0, err
	}
	pn.maxConfiguredReplicaCount, err = pn.decodeToInt32AndError(response, err,
		protocol.PNCounterGetConfiguredReplicaCountDecodeResponse)
	return pn.maxConfiguredReplicaCount, err
}

func (pn *pnCounterProxy) ToVectorClock(replicaLogicalTimestamps []*protocol.Pair) (timestamps *vectorClock) {
	timestamps = newVectorClock()
	for _, pair := range replicaLogicalTimestamps {
		timestamps.SetReplicaTimestamp(pair.Key().(*string), pair.Value().(int64))
	}
	return
}

func (pn *pnCounterProxy) updateObservedReplicaTimestamps(receivedLogicalTimestamps []*protocol.Pair) {
	received := pn.ToVectorClock(receivedLogicalTimestamps)
	for {
		if (*vectorClock)(atomic.LoadPointer(&pn.observedClock)).IsAfter(received) {
			break
		}
		if atomic.CompareAndSwapPointer(&pn.observedClock, atomic.LoadPointer(&pn.observedClock), unsafe.Pointer(received)) {
			break
		}
	}
}
