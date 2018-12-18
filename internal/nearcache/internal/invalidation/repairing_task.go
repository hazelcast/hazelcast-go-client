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

package invalidation

import (
	"time"

	"fmt"

	"log"

	"sync"

	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/clientspi"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

const asyncResultWaitTimeout = time.Duration(1 * time.Second)

type RepairingTask struct {
	partitionCount         int32
	localUUID              string
	reconciliationInterval time.Duration
	serializationService   spi.SerializationService
	partitionService       clientspi.PartitionService
	maxToleratedMissCount  int64
	lastAntiEntropyRun     atomic.Value
	handlers               sync.Map
	closed                 chan struct{}
}

func NewRepairingTask(properties *property.HazelcastProperties, service spi.SerializationService,
	partitionService clientspi.PartitionService, localUUID string) *RepairingTask {
	r := &RepairingTask{
		localUUID:            localUUID,
		serializationService: service,
		partitionService:     partitionService,
		partitionCount:       partitionService.GetPartitionCount(),
	}
	r.initMaxToleratedMissCount(properties)
	err := r.initReconciliationInterval(properties)
	if err != nil {
		return nil
	}
	go r.process()
	return r
}

func (r *RepairingTask) initMaxToleratedMissCount(properties *property.HazelcastProperties) {
	r.maxToleratedMissCount = int64(properties.GetIntOrDefault(property.MaxToleratedMissCount))
}

func (r *RepairingTask) initReconciliationInterval(properties *property.HazelcastProperties) error {
	minReconciliationInterval := properties.GetDuration(property.MinReconciliationIntervalSeconds)
	reconciliationInterval := properties.GetDuration(property.ReconciliationIntervalSeconds)
	if reconciliationInterval < 0 || (reconciliationInterval > 0 && reconciliationInterval < minReconciliationInterval) {
		return core.NewHazelcastIllegalArgumentError(fmt.Sprintf("Reconciliation interval"+
			" can be at least %s seconds if it is not zero, but %s was configured."+
			" Note: Configuring a value of zero seconds disables the reconciliation task.",
			minReconciliationInterval, reconciliationInterval), nil)
	}
	r.reconciliationInterval = reconciliationInterval
	return nil
}

func (r *RepairingTask) process() {

	ticker := time.Tick(time.Second)
	for {
		select {
		case <-ticker:
			r.fixSequenceGaps()
			if r.isAntiEntropyNeeded() {
				r.runAntiEntropy()
			}
		case <-r.closed:
			return
		}
	}
}

func (r *RepairingTask) isAboveMaxToleratedMissCount(handler *RepairingHandler) bool {
	totalMissCount := int64(0)
	for partitionID := int32(0); partitionID < r.partitionCount; partitionID++ {
		metaData := handler.MetaDataContainer(partitionID)
		totalMissCount += metaData.MissedSequenceCount()
		if totalMissCount > r.maxToleratedMissCount {
			// TODO:: log at trace level
			log.Println(fmt.Sprintf("Above tolerated miss count:[map=%s,missCount=%d,maxToleratedMissCount=%d]",
				handler.Name(), totalMissCount, r.maxToleratedMissCount))
			return true
		}
	}
	return false
}

func (r *RepairingTask) updateLastKnownStaleSequences(handler *RepairingHandler) {
	for partitionID := int32(0); partitionID < r.partitionCount; partitionID++ {
		metaData := handler.MetaDataContainer(partitionID)
		missCount := metaData.MissedSequenceCount()
		if missCount != 0 {
			metaData.AddAndGetMissedSequenceCount(-missCount)
			handler.UpdateLastKnownStaleSequence(metaData, partitionID)
		}
	}
}

func (r *RepairingTask) fixSequenceGaps() {
	handlerFunc := func(_, handler interface{}) bool {
		repairingHandler := handler.(*RepairingHandler)
		if r.isAboveMaxToleratedMissCount(repairingHandler) {
			r.updateLastKnownStaleSequences(repairingHandler)
		}
		return true
	}
	r.handlers.Range(handlerFunc)
}

func (r *RepairingTask) runAntiEntropy() {
	r.lastAntiEntropyRun.Store(time.Now())
}

func (r *RepairingTask) isAntiEntropyNeeded() bool {
	if r.reconciliationInterval == 0 {
		return false
	}
	return time.Since(r.lastAntiEntropyRun.Load().(time.Time)) >= r.reconciliationInterval
}
