package internal

import "github.com/hazelcast/go-client"

type PartitionService struct {
	client *HazelcastClient
	partitions map[int32]*hazelcast.Address
	partitionCount int32
}

func NewPartitionService(client *HazelcastClient) *PartitionService {
	return &PartitionService{client:client, partitions:make(map[int32]*hazelcast.Address)}
}

func (partitionService *PartitionService) PartitionCount() int32 {
	return partitionService.partitionCount
}

func (partitionService *PartitionService) PartitionOwner(partitionId int32) (*hazelcast.Address, bool) {
	address, ok := partitionService.partitions[partitionId]
	return address, ok
}

func (partitionService *PartitionService) GetPartitionId(key interface{}) int32 {
	data, error := partitionService.client.serializationService.ToData(key)
	if error != nil {
		//TODO handle error
	}

	count := partitionService.PartitionCount()
	if count <= 0 {
		return 0
	}

	return hashToIndex(data.GetPartitionHash(), count)
}
