package internal

import (
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
)


type PartitionService struct {
	client *HazelcastClient
	partitions map[int32]*Address
	partitionCount int32
}

func NewPartitionService(client *HazelcastClient) *PartitionService {
	return &PartitionService{client:client, partitions:make(map[int32]*Address)}
}

func (partitionService *PartitionService) PartitionCount() int32 {
	return partitionService.partitionCount
}

func (partitionService *PartitionService) PartitionOwner(partitionId int32) (*Address, bool) {
	address, ok := partitionService.partitions[partitionId]
	return address, ok
}

func (partitionService *PartitionService) GetPartitionId(key interface{}) int32 {
	data, error := partitionService.client.SerializationService.ToData(key)
	if error != nil {
		//TODO handle error
	}

	count := partitionService.PartitionCount()
	if count <= 0 {
		return 0
	}

	return common.HashToIndex(data.GetPartitionHash(), count)
}
