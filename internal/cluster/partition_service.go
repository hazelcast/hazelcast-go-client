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

package cluster

import (
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/util/murmur"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

type PartitionServiceCreationBundle struct {
	SerializationService *iserialization.Service
	EventDispatcher      *event.DispatchService
	Logger               ilogger.Logger
}

func (b PartitionServiceCreationBundle) Check() {
	if b.SerializationService == nil {
		panic("SerializationService is nil")
	}
	if b.EventDispatcher == nil {
		panic("EventDispatcher is nil")
	}
	if b.Logger == nil {
		panic("Logger is nil")
	}
}

type PartitionService struct {
	serializationService *iserialization.Service
	eventDispatcher      *event.DispatchService
	partitionTable       partitionTable
	partitionCount       atomic.Value
	logger               ilogger.Logger
}

func NewPartitionService(bundle PartitionServiceCreationBundle) *PartitionService {
	bundle.Check()
	return &PartitionService{
		serializationService: bundle.SerializationService,
		eventDispatcher:      bundle.EventDispatcher,
		partitionTable:       defaultPartitionTable(),
		logger:               bundle.Logger,
	}
}

func (s *PartitionService) Start() {
	subscriptionID := event.MakeSubscriptionID(s.handlePartitionsUpdated)
	s.eventDispatcher.Subscribe(EventPartitionsUpdated, subscriptionID, s.handlePartitionsUpdated)
}

func (s *PartitionService) Stop() {
	subscriptionID := event.MakeSubscriptionID(s.handlePartitionsUpdated)
	s.eventDispatcher.Unsubscribe(EventPartitionsUpdated, subscriptionID)
}

func (s *PartitionService) GetPartitionOwner(partitionId int32) internal.UUID {
	return s.partitionTable.GetOwnerUUID(partitionId)
}

func (s *PartitionService) PartitionCount() int32 {
	return s.partitionCount.Load().(int32)
}

func (s *PartitionService) GetPartitionID(keyData pubserialization.Data) (int32, error) {
	if count := s.PartitionCount(); count == 0 {
		// Partition count can not be zero for the sync mode.
		// On the sync mode, we are waiting for the first connection to be established.
		// We are initializing the partition count with the value coming from the server with authentication.
		// This exception is used only for async mode client.
		return 0, hzerror.ErrClientOffline
	} else {
		return murmur.HashToIndex(keyData.PartitionHash(), count), nil
	}
}

func (s *PartitionService) GetPartitionIDWithKey(key interface{}) (int32, error) {
	if data, err := s.serializationService.ToData(key); err != nil {
		return 0, err
	} else {
		return s.GetPartitionID(data)
	}
}

func (s *PartitionService) handlePartitionsUpdated(event event.Event) {
	if ev, ok := event.(*PartitionsUpdated); ok {
		if s.partitionTable.Update(ev.Partitions, ev.Version, ev.ConnectionID) {
			s.eventDispatcher.Publish(NewPartitionsLoaded())
			s.logger.Debug(func() string { return "partitions loaded" })
		}
	}
}

func (s *PartitionService) checkAndSetPartitionCount(newPartitionCount int32) {
	s.partitionCount.Store(newPartitionCount)
}

type partitionTable struct {
	partitionStateVersion int32
	partitions            map[int32]internal.UUID
	connectionID          int64
	mu                    *sync.RWMutex
}

func (p *partitionTable) Update(pairs []proto.Pair, version int32, connectionID int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(pairs) == 0 || p.connectionID == connectionID && version <= p.partitionStateVersion {
		return false
	}
	newPartitions := map[int32]internal.UUID{}
	for _, pair := range pairs {
		uuids := pair.Key().([]internal.UUID)
		ids := pair.Value().([]int32)
		for _, uuid := range uuids {
			for _, id := range ids {
				newPartitions[id] = uuid
			}
		}
	}
	p.partitions = newPartitions
	p.partitionStateVersion = version
	p.connectionID = connectionID
	return true
}

func (p *partitionTable) GetOwnerUUID(partitionID int32) internal.UUID {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if uuid, ok := p.partitions[partitionID]; ok {
		return uuid
	}
	return nil
}

func (p *partitionTable) PartitionCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.partitions)
}

func defaultPartitionTable() partitionTable {
	return partitionTable{
		partitionStateVersion: -1,
		partitions:            map[int32]internal.UUID{},
		mu:                    &sync.RWMutex{},
	}
}
