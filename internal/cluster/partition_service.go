package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/murmur"
	"sync/atomic"
)

type PartitionService interface {
	PartitionCount() int32
	GetPartitionID(keyData serialization.Data) int32
	GetPartitionIDWithKey(key interface{}) (int32, error)
	GetPartitionOwner(partitionId int32) internal.UUID
}

type PartitionServiceCreationBundle struct {
	SerializationService serialization.Service
	Logger               logger.Logger
}

func (b PartitionServiceCreationBundle) Check() {
	if b.SerializationService == nil {
		panic("SerializationService is nil")
	}
	if b.Logger == nil {
		panic("Logger is nil")
	}
}

type PartitionServiceImpl struct {
	serializationService serialization.Service
	partitionTable       atomic.Value
	partitionCount       uint32
	logger               logger.Logger
}

func NewPartitionServiceImpl(bundle PartitionServiceCreationBundle) *PartitionServiceImpl {
	bundle.Check()
	service := &PartitionServiceImpl{
		serializationService: bundle.SerializationService,
		logger:               bundle.Logger,
	}
	service.partitionTable.Store(defaultPartitionTable())
	return service
}

func (s *PartitionServiceImpl) GetPartitionOwner(partitionId int32) internal.UUID {
	return s.partitionTable.Load().(partitionTable).partitions[partitionId]
}

func (s *PartitionServiceImpl) PartitionCount() int32 {
	return int32(atomic.LoadUint32(&s.partitionCount))
}

func (s *PartitionServiceImpl) GetPartitionID(keyData serialization.Data) int32 {
	if count := s.PartitionCount(); count == 0 {
		// Partition count can not be zero for the sync mode.
		// On the sync mode, we are waiting for the first connection to be established.
		// We are initializing the partition count with the value coming from the server with authentication.
		// This exception is used only for async mode client.
		// TODO: panic
		//core.NewHazelcastClientOfflineException()
		return 0
	} else {
		return murmur.HashToIndex(keyData.PartitionHash(), count)
	}
}

func (s *PartitionServiceImpl) GetPartitionIDWithKey(key interface{}) (int32, error) {
	if data, err := s.serializationService.ToData(key); err != nil {
		return 0, err
	} else {
		return s.GetPartitionID(data), nil
	}
}

func (s *PartitionServiceImpl) checkAndSetPartitionCount(newPartitionCount int32) {
	atomic.CompareAndSwapUint32(&s.partitionCount, 0, uint32(newPartitionCount))
}

type partitionTable struct {
	//connection           *connection.Impl
	partitionStateVersion int32
	partitions            map[int32]internal.UUID
}

func defaultPartitionTable() partitionTable {
	//return partitionTable{nil, -1, map[int32]core.UUID{}}
	return partitionTable{
		partitionStateVersion: -1,
		partitions:            map[int32]internal.UUID{},
	}
}

//func newPartitionTable(connection *connection.Impl, partitionSateVersion int32, partitions map[int32]core.UUID) partitionTable {
//	return partitionTable{connection: connection, partitionSateVersion: partitionSateVersion, partitions: partitions}
//}
