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
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
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
	handlers               *sync.Map
	metaDataFetcher        *MetaDataFetcher
	closed                 chan struct{}
}

func NewRepairingTask(properties *property.HazelcastProperties, service spi.SerializationService,
	partitionService clientspi.PartitionService, invocationService clientspi.InvocationService,
	cluster core.Cluster, localUUID string) *RepairingTask {
	r := &RepairingTask{
		localUUID:            localUUID,
		serializationService: service,
		partitionService:     partitionService,
		partitionCount:       partitionService.GetPartitionCount(),
		closed:               make(chan struct{}, 0),
		handlers:             new(sync.Map),
	}
	r.initMetaDataFetcher(invocationService, cluster)
	r.lastAntiEntropyRun.Store(time.Now())
	r.initMaxToleratedMissCount(properties)
	err := r.initReconciliationInterval(properties)
	if err != nil {
		return nil
	}
	go r.process()
	return r
}

func (r *RepairingTask) initMetaDataFetcher(service clientspi.InvocationService, cluster core.Cluster) {
	r.metaDataFetcher = NewMetaDataFetcher(service, cluster, r.handlers)
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

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			r.fixSequenceGaps()
			if r.isAntiEntropyNeeded() {
				r.runAntiEntropy()
			}
		case <-r.closed:
			ticker.Stop()
			return
		}
	}
}

func (r *RepairingTask) RegisterAndGetHandler(dataStructureName string, cache nearcache.NearCache) nearcache.RepairingHandler {
	if handler, found := r.handlers.Load(dataStructureName); found {
		return handler.(*RepairingHandler)
	}
	handler := r.createNewHandler(dataStructureName, cache)
	r.handlers.Store(dataStructureName, handler)
	return handler
}

func (r *RepairingTask) createNewHandler(dataStructureName string, cache nearcache.NearCache) *RepairingHandler {
	handler := NewRepairingHandler(r.localUUID, dataStructureName, cache, r.serializationService, r.partitionService)
	staleReadDetector := NewDefaultStaleReadDetector(handler, r.partitionService)
	cache.SetStaleReadDetector(staleReadDetector)
	r.initRepairingHandler(handler)
	return handler
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
	r.metaDataFetcher.fetchMetaData()
	r.lastAntiEntropyRun.Store(time.Now())
}

func (r *RepairingTask) isAntiEntropyNeeded() bool {
	if r.reconciliationInterval == 0 {
		return false
	}
	return time.Since(r.lastAntiEntropyRun.Load().(time.Time)) >= r.reconciliationInterval
}

func (r *RepairingTask) initRepairingHandler(handler *RepairingHandler) {
	r.metaDataFetcher.init(handler)
}

func (r *RepairingTask) Shutdown() {
	close(r.closed)
}
