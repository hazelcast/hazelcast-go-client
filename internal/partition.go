package internal

import (
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"
	"sync"
	"time"
)

const PARTITION_UPDATE_INTERVAL = 10

type PartitionService struct {
	client         *HazelcastClient
	partitions     map[int32]*Address
	partitionCount int32
	mu             sync.Mutex
}

func NewPartitionService(client *HazelcastClient) *PartitionService {
	return &PartitionService{client: client, partitions: make(map[int32]*Address)}
}
func (partitionService *PartitionService) start() {
	partitionService.doRefresh()
	go func() {
		partitionService.doRefresh()
		time.Sleep(time.Duration(PARTITION_UPDATE_INTERVAL * time.Second))
	}()

}
func (partitionService *PartitionService) PartitionCount() int32 {
	partitionService.mu.Lock()
	defer partitionService.mu.Unlock()
	return int32(len(partitionService.partitions))
}

func (partitionService *PartitionService) PartitionOwner(partitionId int32) (*Address, bool) {
	address, ok := partitionService.partitions[partitionId]
	return address, ok
}

func (partitionService *PartitionService) GetPartitionId(key interface{}) int32 {
	/*
		data, error := partitionService.client.SerializationService.ToData(key)
		if error != nil {
			//TODO handle error
		}
	*/
	data := serialization.Data{[]byte("askljjjlkklds")}

	count := partitionService.PartitionCount()
	if count <= 0 {
		return 0
	}
	return common.HashToIndex(data.GetPartitionHash(), count)
}
func (partitionService *PartitionService) doRefresh() {
	partitionService.mu.Lock()
	defer partitionService.mu.Unlock()
	address := partitionService.client.ClusterService.ownerConnectionAddress
	connectionChan := partitionService.client.ConnectionManager.GetConnection(address)
	connection := <-connectionChan
	if connection == nil {
		//TODO:: Handle error
	}
	request := ClientGetPartitionsEncodeRequest()
	result, err := partitionService.client.InvocationService.InvokeOnConnection(request, connection).Result()
	if err != nil {
		//TODO:: Handle error
	}
	partitionService.processPartitionResponse(result)

}
func (partitionService *PartitionService) processPartitionResponse(result *ClientMessage) {
	partitions := ClientGetPartitionsDecodeResponse(result).Partitions
	partitionService.partitions = make(map[int32]*Address)
	for _, partitionList := range partitions {
		addr := partitionList.Key().(*Address)
		for _, partition := range partitionList.Value().([]int32) {
			partitionService.partitions[int32(partition)] = addr
		}
	}
}
