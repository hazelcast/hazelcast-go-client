// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"log"
	"sync/atomic"
	"time"
)

const PartitionUpdateInterval time.Duration = 5

type partitionService struct {
	client         *HazelcastClient
	mp             atomic.Value
	partitionCount int32
	cancel         chan struct{}
	refresh        chan struct{}
}

func newPartitionService(client *HazelcastClient) *partitionService {
	return &partitionService{client: client, cancel: make(chan struct{}), refresh: make(chan struct{}, 1)}
}

func (partitionService *partitionService) start() {
	partitionService.doRefresh()
	go func() {
		ticker := time.NewTicker(PartitionUpdateInterval * time.Second)
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
func (partitionService *partitionService) getPartitionCount() int32 {
	partitions := partitionService.mp.Load().(map[int32]*Address)
	return int32(len(partitions))
}
func (partitionService *partitionService) partitionOwner(partitionId int32) (*Address, bool) {
	partitions := partitionService.mp.Load().(map[int32]*Address)
	address, ok := partitions[partitionId]
	return address, ok
}

func (partitionService *partitionService) GetPartitionId(keyData *serialization.Data) int32 {
	count := partitionService.getPartitionCount()
	if count <= 0 {
		return 0
	}
	return common.HashToIndex(keyData.GetPartitionHash(), count)
}

func (partitionService *partitionService) getPartitionIdWithKey(key interface{}) (int32, error) {
	data, err := partitionService.client.SerializationService.ToData(key)
	if err != nil {
		return 0, err
	}
	return partitionService.GetPartitionId(data), nil
}

func (partitionService *partitionService) doRefresh() {
	connection := partitionService.client.ConnectionManager.getOwnerConnection()
	if connection == nil {
		log.Println("error while fetching cluster partition table!")
		return
	}
	request := ClientGetPartitionsEncodeRequest()
	result, err := partitionService.client.InvocationService.invokeOnConnection(request, connection).Result()
	if err != nil {
		log.Println("error while fetching cluster partition table! ", err)
		return
	}
	partitionService.processPartitionResponse(result)
}

func (partitionService *partitionService) processPartitionResponse(result *ClientMessage) {
	partitions /*partitionStateVersion*/, _ := ClientGetPartitionsDecodeResponse(result)()
	newPartitions := make(map[int32]*Address, len(partitions))
	for _, partitionList := range partitions {
		addr := partitionList.Key().(*Address)
		for _, partition := range partitionList.Value().([]int32) {
			newPartitions[int32(partition)] = addr
		}
	}
	partitionService.mp.Store(newPartitions)
}

func (partitionService *partitionService) shutdown() {
	close(partitionService.cancel)
}
