package partition

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/murmur"
	"sync/atomic"
)

type Service interface {
	GetPartitionCount() int32
	GetPartitionID(keyData serialization.Data) int32
	GetPartitionIDWithKey(key interface{}) (int32, error)
	GetPartitionOwner(partitionId int32) core.UUID
}

type ServiceImpl struct {
	serializationService serialization.Service
	partitionTable       atomic.Value
	partitionCount       uint32
	logger               logger.Logger
}

func NewServiceImpl(bundle CreationBundle) *ServiceImpl {
	bundle.Check()
	service := &ServiceImpl{
		serializationService: bundle.SerializationService,
		logger:               bundle.Logger,
	}
	service.partitionTable.Store(defaultPartitionTable())
	return service
}

func (s *ServiceImpl) GetPartitionOwner(partitionId int32) core.UUID {
	return s.partitionTable.Load().(partitionTable).partitions[partitionId]
}

func (s *ServiceImpl) GetPartitionCount() int32 {
	return int32(atomic.LoadUint32(&s.partitionCount))
}

func (s *ServiceImpl) GetPartitionID(keyData serialization.Data) int32 {
	if count := s.GetPartitionCount(); count == 0 {
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

func (s *ServiceImpl) GetPartitionIDWithKey(key interface{}) (int32, error) {
	if data, err := s.serializationService.ToData(key); err != nil {
		return 0, err
	} else {
		return s.GetPartitionID(data), nil
	}
}

type partitionTable struct {
	//connection           *connection.Impl
	partitionStateVersion int32
	partitions            map[int32]core.UUID
}

func defaultPartitionTable() partitionTable {
	//return partitionTable{nil, -1, map[int32]core.UUID{}}
	return partitionTable{
		partitionStateVersion: -1,
		partitions:            map[int32]core.UUID{},
	}
}

//func newPartitionTable(connection *connection.Impl, partitionSateVersion int32, partitions map[int32]core.UUID) partitionTable {
//	return partitionTable{connection: connection, partitionSateVersion: partitionSateVersion, partitions: partitions}
//}
