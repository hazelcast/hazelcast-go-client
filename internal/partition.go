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

package internal

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/util/murmur"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"reflect"
	"sync/atomic"
)

type partitionService struct {
	client         *HazelcastClient
	partitionTable atomic.Value
	partitionCount uint32
	logger         logger.Logger
}

func newPartitionService(client *HazelcastClient) *partitionService {
	partitionService := partitionService{client: client, partitionCount: 0, logger: client.logger}
	partitionService.partitionTable.Store(defaultPartitionTable())
	return &partitionService
}

func (ps *partitionService) handlePartitionsViewEvent(connection *Connection, partitionStateVersion int32, partitions []proto.Pair) {
	for {
		current := ps.partitionTable.Load().(partitionTable)
		if !shouldBeApplied(ps, connection, partitions, partitionStateVersion, current) {
			return
		}

		newPartitions := convertToMap(partitions)
		newPartitionTable := newPartitionTable(connection, partitionStateVersion, newPartitions)

		if reflect.DeepEqual(ps.partitionTable.Load().(partitionTable), current) {
			ps.partitionTable.Store(newPartitionTable)
			ps.logger.Debug("Applied partition table with partitionStateVersion : ", partitionStateVersion)
			return
		}
	}
}

func (ps *partitionService) reset() {
	ps.partitionTable.Store(defaultPartitionTable())
}

func (ps *partitionService) GetPartitionOwner(partitionId int32) core.UUID {
	return ps.partitionTable.Load().(partitionTable).partitions[partitionId]
}

func (ps *partitionService) GetPartitions() map[int32]core.UUID {
	return ps.partitionTable.Load().(partitionTable).partitions
}

func (ps *partitionService) GetPartitionID(keyData serialization.Data) int32 {
	count := ps.GetPartitionCount()
	if count == 0 {
		// Partition count can not be zero for the sync mode.
		// On the sync mode, we are waiting for the first connection to be established.
		// We are initializing the partition count with the value coming from the server with authentication.
		// This exception is used only for async mode client.
		// TODO throw exception
		//core.NewHazelcastClientOfflineException()
		return 0
	}
	return murmur.HashToIndex(keyData.GetPartitionHash(), count)
}

func (ps *partitionService) GetPartitionIDWithKey(key interface{}) (int32, error) {
	data, err := ps.client.SerializationService.ToData(key)
	if err != nil {
		return 0, err
	}
	return ps.GetPartitionID(data), nil
}

func (ps *partitionService) GetPartitionCount() int32 {
	return int32(atomic.LoadUint32(&ps.partitionCount))
}

func (ps *partitionService) checkAndSetPartitionCount(newPartitionCount int32) {
	atomic.CompareAndSwapUint32(&ps.partitionCount, 0, uint32(newPartitionCount))
}

func convertToMap(partitions []proto.Pair) map[int32]core.UUID {
	newPartitions := make(map[int32]core.UUID)
	for _, partition := range partitions {
		uuids := partition.Key().([]core.UUID)
		ownedPartitions := partition.Value().([]int32)

		for _, eachUUID := range uuids {
			for _, ownedPartition := range ownedPartitions {
				newPartitions[ownedPartition] = eachUUID
			}
		}
	}

	return newPartitions
}

func shouldBeApplied(ps *partitionService, connection *Connection, partitions []proto.Pair, partitionStateVersion int32, current partitionTable) bool {
	if len(partitions) == 0 {
		logFailure(ps, connection, partitionStateVersion, current, "response is empty")
		return false
	}

	if !reflect.DeepEqual(connection, current.connection) {
		ps.logger.Debug("Event coming from a new connection. Old connection: ", " ", ", new connection ", "")
		return true
	}

	if partitionStateVersion <= current.partitionSateVersion {
		logFailure(ps, connection, partitionStateVersion, current, "response partition state version is old")
		return false
	}

	return true
}

func logFailure(ps *partitionService, connection *Connection, partitionStateVersion int32, current partitionTable, cause string) {
	ps.logger.Debug(" We will not apply the response, since ", cause,
		" . Response is from ", connection,
		". Current connection ", current.connection, " response state version:", partitionStateVersion,
		". Current state version: ", current.partitionSateVersion)
}

type partitionTable struct {
	connection           *Connection
	partitionSateVersion int32
	partitions           map[int32]core.UUID
}

func defaultPartitionTable() partitionTable {
	return partitionTable{nil, -1, make(map[int32]core.UUID)}
}

func newPartitionTable(connection *Connection, partitionSateVersion int32, partitions map[int32]core.UUID) partitionTable {
	return partitionTable{connection: connection, partitionSateVersion: partitionSateVersion, partitions: partitions}
}
