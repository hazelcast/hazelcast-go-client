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

package invalidation

import (
	"fmt"
	"log"
	"sync"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type MetaDataFetcher struct {
	client   *internal.HazelcastClient
	handlers sync.Map
}

func (i *MetaDataFetcher) fetchMetaData() {

	members := i.client.ClusterService.GetMembers()
	names := i.dataStructureNames(i.handlers)
	for _, member := range members {
		if member.IsLiteMember() {
			continue
		}
		i.fetchMetaDataFor(names, member)
	}
}

func (i *MetaDataFetcher) fetchMetaDataFor(names []string, member core.Member) {
	address := member.Address()
	request := proto.MapFetchNearCacheInvalidationMetadataEncodeRequest(names, address.(*proto.Address))
	responseMessage, err := i.client.InvocationService().InvokeOnTarget(
		request, address).ResultWithTimeout(asyncResultWaitTimeout)
	if err != nil {
		// TODO:: log at warning level
		log.Println(fmt.Sprintf("Cannot fetch invalidation meta-data from address %s, %s", address, err))
	} else {
		namePartitionSequenceList, partitionUUIDList :=
			proto.MapFetchNearCacheInvalidationMetadataDecodeResponse(responseMessage)()
		i.repairUUIDs(partitionUUIDList)
		i.repairSequences(namePartitionSequenceList)
	}
}

func (i *MetaDataFetcher) dataStructureNames(handlers sync.Map) []string {
	names := make([]string, 0)
	nameFinder := func(_, handler interface{}) bool {
		repairHandler := handler.(*RepairingHandler)
		names = append(names, repairHandler.Name())
		return true
	}
	handlers.Range(nameFinder)
	return names
}

func (i *MetaDataFetcher) repairUUIDs(partitionUUIDList []*proto.Pair) {
	for _, UUID := range partitionUUIDList {
		handlerFunc := func(_, handler interface{}) bool {
			repairHandler := handler.(*RepairingHandler)
			repairHandler.CheckOrRepairUUID(UUID.Key().(int32), UUID.Value().(string))
			return true
		}
		i.handlers.Range(handlerFunc)
	}
}

func (i *MetaDataFetcher) repairSequences(namePartitionSequenceList []*proto.Pair) {
	for _, namePartitionPair := range namePartitionSequenceList {
		partitionSequenceList := namePartitionPair.Value().([]*proto.Pair)
		for _, partitionSequencePair := range partitionSequenceList {
			if repairingHandler, found := i.handlers.Load(namePartitionPair.Key().(string)); found {
				repairingHandler.(*RepairingHandler).CheckOrRepairSequence(partitionSequencePair.Key().(int32),
					partitionSequencePair.Value().(int64), true)
			}
		}
	}
}
