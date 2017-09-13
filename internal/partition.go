package internal

import (
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const PARTITION_UPDATE_INTERVAL time.Duration = 5

type PartitionService struct {
	client         *HazelcastClient
	partitions     map[int32]*Address
	mapPointer     unsafe.Pointer
	partitionCount int32
	mu             sync.Mutex
	cancel         chan bool
	refresh        chan bool
}

func NewPartitionService(client *HazelcastClient) *PartitionService {
	partitions := make(map[int32]*Address)
	return &PartitionService{client: client, partitions: partitions, cancel: make(chan bool, 0), refresh: make(chan bool, 1),
		mapPointer: unsafe.Pointer(&partitions),
	}
}
func (partitionService *PartitionService) periodicalRefresh() {
	for {
		select {
		case <-time.After(PARTITION_UPDATE_INTERVAL * time.Second):
			partitionService.refresh <- true
		case <-partitionService.cancel:
			return
		}
	}
}
func (partitionService *PartitionService) start() {
	go partitionService.periodicalRefresh()
	go func() {
		for {
			select {
			case <-partitionService.refresh:
				partitionService.doRefresh()
			case <-partitionService.cancel:
				return
			}
		}
	}()
	partitionService.refresh <- true

}
func (partitionService *PartitionService) PartitionCount() int32 {
	partitions := (*map[int32]*Address)(atomic.LoadPointer(&partitionService.mapPointer))
	return int32(len(*partitions))
}
func (partitionService *PartitionService) PartitionOwner(partitionId int32) (*Address, bool) {
	partitions := *(*map[int32]*Address)(atomic.LoadPointer(&partitionService.mapPointer))
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
	connectionChan := partitionService.client.ConnectionManager.GetConnection(address)
	connection, alive := <-connectionChan
	if !alive {
		log.Print("connection is closed")
		//TODO::Handle connection closed
		return
	}
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
	newPartitions := make(map[int32]*Address)
	for _, partitionList := range *partitions {
		addr := partitionList.Key().(*Address)
		for _, partition := range partitionList.Value().([]int32) {
			newPartitions[int32(partition)] = addr
		}
	}
	atomic.StorePointer(&partitionService.mapPointer, unsafe.Pointer(&newPartitions))
}
func (partitionService *PartitionService) shutdown() {
	close(partitionService.cancel)
}
