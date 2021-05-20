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
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/util/murmur"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type PartitionServiceCreationBundle struct {
	EventDispatcher *event.DispatchService
	Logger          ilogger.Logger
}

func (b PartitionServiceCreationBundle) Check() {
	if b.EventDispatcher == nil {
		panic("EventDispatcher is nil")
	}
	if b.Logger == nil {
		panic("Logger is nil")
	}
}

type PartitionService struct {
	eventDispatcher *event.DispatchService
	partitionTable  partitionTable
	partitionCount  int32
	logger          ilogger.Logger
}

func NewPartitionService(bundle PartitionServiceCreationBundle) *PartitionService {
	bundle.Check()
	return &PartitionService{
		eventDispatcher: bundle.EventDispatcher,
		partitionTable:  defaultPartitionTable(),
		logger:          bundle.Logger,
	}
}

func (s *PartitionService) GetPartitionOwner(partitionId int32) *types.UUID {
	return s.partitionTable.GetOwnerUUID(partitionId)
}

func (s *PartitionService) PartitionCount() int32 {
	return atomic.LoadInt32(&s.partitionCount)
}

func (s *PartitionService) GetPartitionID(keyData pubserialization.Data) (int32, error) {
	if count := s.PartitionCount(); count == 0 {
		// Partition count can not be zero for the sync mode.
		// On the sync mode, we are waiting for the first connection to be established.
		// We are initializing the partition count with the value coming from the server with authentication.
		// This exception is used only for async mode client.
		return 0, hzerrors.ErrClientOffline
	} else {
		return murmur.HashToIndex(keyData.PartitionHash(), count), nil
	}
}

func (s *PartitionService) Update(connID int64, partitions []proto.Pair, version int32) {
	if s.partitionTable.Update(partitions, version, connID) {
		s.logger.Debug(func() string { return "partitions updated" })
	}
}

func (s *PartitionService) checkAndSetPartitionCount(newPartitionCount int32) error {
	if atomic.CompareAndSwapInt32(&s.partitionCount, 0, newPartitionCount) {
		return nil
	}
	if atomic.LoadInt32(&s.partitionCount) != newPartitionCount {
		return hzerrors.ErrClientNotAllowedInCluster
	}
	return nil
}

type partitionTable struct {
	partitionStateVersion int32
	partitions            map[int32]types.UUID
	connectionID          int64
	mu                    *sync.RWMutex
}

func (p *partitionTable) Update(pairs []proto.Pair, version int32, connectionID int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	cantApply := len(pairs) == 0 || p.connectionID == connectionID && version <= p.partitionStateVersion
	if cantApply {
		return false
	}
	newPartitions := map[int32]types.UUID{}
	for _, pair := range pairs {
		uuids := pair.Key().([]types.UUID)
		ids := pair.Value().([]int32)
		for _, uuid := range uuids {
			for _, id := range ids {
				newPartitions[id] = uuid
			}
		}
	}
	if reflect.DeepEqual(p.partitions, newPartitions) {
		return false
	}
	p.partitions = newPartitions
	p.partitionStateVersion = version
	p.connectionID = connectionID
	return true
}

func (p *partitionTable) GetOwnerUUID(partitionID int32) *types.UUID {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if uuid, ok := p.partitions[partitionID]; ok {
		uuidCopy := uuid
		return &uuidCopy
	}
	return nil
}

func defaultPartitionTable() partitionTable {
	return partitionTable{
		partitionStateVersion: -1,
		partitions:            map[int32]types.UUID{},
		mu:                    &sync.RWMutex{},
	}
}
