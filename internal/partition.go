package internal

import (
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"
	"log"
	"sync/atomic"
	"time"
)

const PARTITION_UPDATE_INTERVAL time.Duration = 5

type PartitionService struct {
	client         *HazelcastClient
	mp             atomic.Value
	partitionCount int32
	cancel         chan struct{}
	refresh        chan bool
}

func NewPartitionService(client *HazelcastClient) *PartitionService {
	return &PartitionService{client: client, cancel: make(chan struct{}), refresh: make(chan bool, 1)}
}

func (partitionService *PartitionService) start() {
	partitionService.doRefresh()
	go func() {
		ticker := time.NewTicker(PARTITION_UPDATE_INTERVAL * time.Second)
		for {
			select {
			case <-ticker.C:
				partitionService.doRefresh()
			case <-partitionService.refresh:
				partitionService.doRefresh()
			case <-partitionService.cancel:
				ticker.Stop()
				return
			}
		}
	}()

}
func (partitionService *PartitionService) PartitionCount() int32 {
	partitions := partitionService.mp.Load().(map[int32]*Address)
	return int32(len(partitions))
}
func (partitionService *PartitionService) PartitionOwner(partitionId int32) (*Address, bool) {
	partitions := partitionService.mp.Load().(map[int32]*Address)
	address, ok := partitions[partitionId]
	return address, ok
}

func (partitionService *PartitionService) GetPartitionId(keyData *serialization.Data) int32 {
	count := partitionService.PartitionCount()
	if count <= 0 {
		return 0
	}
	return common.HashToIndex(keyData.GetPartitionHash(), count)
}

func (partitionService *PartitionService) doRefresh() {
	address := partitionService.client.ClusterService.ownerConnectionAddress
	connectionChan, errChannel := partitionService.client.ConnectionManager.GetConnection(address)
	var connection *Connection
	var alive bool
	select {
	case connection, alive = <-connectionChan:
		if !alive {
			log.Println("Connection is closed")
			return
		}
	case <-errChannel:
		return
	}
	if connection == nil {
		return
	}
	request := ClientGetPartitionsEncodeRequest()
	result, err := partitionService.client.InvocationService.InvokeOnConnection(request, connection).Result()
	if err != nil {
		return
	}
	partitionService.processPartitionResponse(result)
}
func (partitionService *PartitionService) processPartitionResponse(result *ClientMessage) {
	partitions := ClientGetPartitionsDecodeResponse(result).Partitions
	newPartitions := make(map[int32]*Address, len(*partitions))
	for _, partitionList := range *partitions {
		addr := partitionList.Key().(*Address)
		for _, partition := range partitionList.Value().([]int32) {
			newPartitions[int32(partition)] = addr
		}
	}
	partitionService.mp.Store(newPartitions)
}
func (partitionService *PartitionService) shutdown() {
	close(partitionService.cancel)
}
