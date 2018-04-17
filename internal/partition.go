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
	"log"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const PartitionUpdateInterval = 5 * time.Second

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

func (ps *partitionService) start() {
	ps.doRefresh()
	go func() {
		ticker := time.NewTicker(PartitionUpdateInterval)
		for {
			select {
			case <-ticker.C:
				ps.doRefresh()
			case <-ps.refresh:
				ps.doRefresh()
			case <-ps.cancel:
				ticker.Stop()
				return
			}
		}
	}()

}

func (ps *partitionService) getPartitionCount() int32 {
	partitions := ps.mp.Load().(map[int32]*protocol.Address)
	return int32(len(partitions))
}

func (ps *partitionService) partitionOwner(partitionId int32) (*protocol.Address, bool) {
	partitions := ps.mp.Load().(map[int32]*protocol.Address)
	address, ok := partitions[partitionId]
	return address, ok
}

func (ps *partitionService) GetPartitionId(keyData *serialization.Data) int32 {
	count := ps.getPartitionCount()
	if count <= 0 {
		return 0
	}
	return common.HashToIndex(keyData.GetPartitionHash(), count)
}

func (ps *partitionService) getPartitionIdWithKey(key interface{}) (int32, error) {
	data, err := ps.client.SerializationService.ToData(key)
	if err != nil {
		return 0, err
	}
	return ps.GetPartitionId(data), nil
}

func (ps *partitionService) doRefresh() {
	connection := ps.client.ConnectionManager.getOwnerConnection()
	if connection == nil {
		log.Println("error while fetching cluster partition table!")
		return
	}
	request := protocol.ClientGetPartitionsEncodeRequest()
	result, err := ps.client.InvocationService.invokeOnConnection(request, connection).Result()
	if err != nil {
		log.Println("error while fetching cluster partition table! ", err)
		return
	}
	ps.processPartitionResponse(result)
}

func (ps *partitionService) processPartitionResponse(result *protocol.ClientMessage) {
	partitions /*partitionStateVersion*/, _ := protocol.ClientGetPartitionsDecodeResponse(result)()
	newPartitions := make(map[int32]*protocol.Address, len(partitions))
	for _, partitionList := range partitions {
		addr := partitionList.Key().(*protocol.Address)
		for _, partition := range partitionList.Value().([]int32) {
			newPartitions[int32(partition)] = addr
		}
	}
	ps.mp.Store(newPartitions)
}

func (ps *partitionService) shutdown() {
	close(ps.cancel)
}
