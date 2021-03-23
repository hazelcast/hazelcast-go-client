package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/murmur"
	"sync"
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
	EventDispatcher      event.DispatchService
	Logger               logger.Logger
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

type PartitionServiceImpl struct {
	serializationService serialization.Service
	eventDispatcher      event.DispatchService
	//partitionTable       atomic.Value
	partitionTable partitionTable
	partitionCount uint32
	logger         logger.Logger
}

func NewPartitionServiceImpl(bundle PartitionServiceCreationBundle) *PartitionServiceImpl {
	bundle.Check()
	return &PartitionServiceImpl{
		serializationService: bundle.SerializationService,
		eventDispatcher:      bundle.EventDispatcher,
		partitionTable:       defaultPartitionTable(),
		logger:               bundle.Logger,
	}
}

func (s *PartitionServiceImpl) Start() {
	subscriptionID := event.MakeSubscriptionID(s.handlePartitionsUpdated)
	s.eventDispatcher.Subscribe(EventPartitionsUpdated, subscriptionID, s.handlePartitionsUpdated)
}

func (s *PartitionServiceImpl) Stop() {
	subscriptionID := event.MakeSubscriptionID(s.handlePartitionsUpdated)
	s.eventDispatcher.Unsubscribe(EventPartitionsUpdated, subscriptionID)
}

func (s *PartitionServiceImpl) GetPartitionOwner(partitionId int32) internal.UUID {
	return s.partitionTable.GetOwnerUUID(partitionId)
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

func (s *PartitionServiceImpl) handlePartitionsUpdated(event event.Event) {
	if partitionsUpdatedEvent, ok := event.(PartitionsUpdated); ok {
		s.logger.Info("partitions updated")
		s.partitionTable.Update(partitionsUpdatedEvent.Partitions(), partitionsUpdatedEvent.Version())
	}
}

func (s *PartitionServiceImpl) checkAndSetPartitionCount(newPartitionCount int32) {
	atomic.CompareAndSwapUint32(&s.partitionCount, 0, uint32(newPartitionCount))
}

type partitionTable struct {
	//connection           *connection.Impl
	partitionStateVersion int32
	partitions            map[int32]internal.UUID
	mu                    *sync.RWMutex
}

func (p *partitionTable) Update(pairs []proto.Pair, version int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if version > p.partitionStateVersion {
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
	}
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
	//return partitionTable{nil, -1, map[int32]core.UUID{}}
	return partitionTable{
		partitionStateVersion: -1,
		partitions:            map[int32]internal.UUID{},
		mu:                    &sync.RWMutex{},
	}
}

//func newPartitionTable(connection *connection.Impl, partitionSateVersion int32, partitions map[int32]core.UUID) partitionTable {
//	return partitionTable{connection: connection, partitionSateVersion: partitionSateVersion, partitions: partitions}
//}
