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
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/util/murmur"
	"github.com/hazelcast/hazelcast-go-client/serialization"
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
	logger          ilogger.Logger
	eventDispatcher *event.DispatchService
	partitionTable  partitionTable
	partitionCount  int32
}

func NewPartitionService(bundle PartitionServiceCreationBundle) *PartitionService {
	bundle.Check()
	return &PartitionService{
		eventDispatcher: bundle.EventDispatcher,
		partitionTable:  defaultPartitionTable(),
		logger:          bundle.Logger,
	}
}

func (s *PartitionService) GetPartitionOwner(partitionId int32) (types.UUID, bool) {
	return s.partitionTable.GetOwnerUUID(partitionId)
}

func (s *PartitionService) PartitionCount() int32 {
	return atomic.LoadInt32(&s.partitionCount)
}

func (s *PartitionService) GetPartitionID(keyData serialization.Data) (int32, error) {
	if count := s.PartitionCount(); count == 0 {
		// Partition count can not be zero for the sync mode.
		// On the sync mode, we are waiting for the first connection to be established.
		// We are initializing the partition count with the value coming from the server with authentication.
		// This exception is used only for async mode client.
		return 0, hzerrors.ErrClientOffline
	} else {
		return murmur.HashToIndex(iserialization.DataPartitionHashFor(keyData), count), nil
	}
}

func (s *PartitionService) Update(connID int64, partitions []proto.Pair, version int32) {
	if s.partitionTable.Update(partitions, version, connID) {
		s.logger.Debug(func() string { return "partitions updated" })
	}
}

func (s *PartitionService) Reset() {
	s.partitionTable.Reset()
}

func (s *PartitionService) checkAndSetPartitionCount(newPartitionCount int32) error {
	if atomic.CompareAndSwapInt32(&s.partitionCount, 0, newPartitionCount) {
		return nil
	}
	if atomic.LoadInt32(&s.partitionCount) != newPartitionCount {
		return fmt.Errorf("client cannot work with this cluster because it has different partition count, expected %d, got %d: %w",
			atomic.LoadInt32(&s.partitionCount), newPartitionCount, hzerrors.ErrClientNotAllowedInCluster)
	}
	return nil
}

type partitionTable struct {
	partitions            map[int32]types.UUID
	mu                    *sync.RWMutex
	connectionID          int64
	partitionStateVersion int32
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

func (p *partitionTable) GetOwnerUUID(partitionID int32) (types.UUID, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if uuid, ok := p.partitions[partitionID]; ok {
		return uuid, true
	}
	return types.UUID{}, false
}

func (p *partitionTable) Reset() {
	p.mu.Lock()
	p.partitionStateVersion = -1
	p.partitions = map[int32]types.UUID{}
	p.connectionID = 0
	p.mu.Unlock()
}

func defaultPartitionTable() partitionTable {
	return partitionTable{
		partitionStateVersion: -1,
		partitions:            map[int32]types.UUID{},
		mu:                    &sync.RWMutex{},
	}
}
