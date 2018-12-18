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
	"log"

	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal/clientspi"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

type RepairingHandler struct {
	partitionCount       int32
	serializeKeys        bool
	localUUID            string
	name                 string
	nearCache            nearcache.NearCache
	serializationService spi.SerializationService
	partitionService     clientspi.PartitionService
	metaDataContainers   []*MetaDataContainer
}

func NewRepairingHandler(localUUID string, name string, nearCache nearcache.NearCache, service spi.SerializationService,
	partitionService clientspi.PartitionService) *RepairingHandler {
	r := &RepairingHandler{
		localUUID:            localUUID,
		name:                 name,
		nearCache:            nearCache,
		serializationService: service,
		partitionService:     partitionService,
		partitionCount:       partitionService.GetPartitionCount(),
	}
	r.metaDataContainers = r.createMetadataContainers(r.partitionCount)
	return r
}

func (r *RepairingHandler) createMetadataContainers(partitionCount int32) []*MetaDataContainer {
	metaDataContainers := make([]*MetaDataContainer, partitionCount)
	for index := 0; index < int(partitionCount); index++ {
		metaDataContainers[index] = NewMetaDataContainer()
	}
	return metaDataContainers
}

func (r *RepairingHandler) Name() string {
	return r.name
}

func (r *RepairingHandler) InitUUID(partitionID int32, partitionUUID string) {
	metaData := r.MetaDataContainer(partitionID)
	metaData.SetUUID(partitionUUID)
}

func (r *RepairingHandler) InitSequence(partitionID int32, partitionSeq int64) {
	metaData := r.MetaDataContainer(partitionID)
	metaData.SetSequence(partitionSeq)
}

func (r *RepairingHandler) MetaDataContainer(partitionID int32) *MetaDataContainer {
	return r.metaDataContainers[partitionID]
}

func (r *RepairingHandler) CheckOrRepairUUID(partition int32, newUUID string) {
	metaData := r.MetaDataContainer(partition)
	for {
		prevUUID := metaData.UUID()
		if prevUUID != "" && prevUUID == newUUID {
			break
		}
		if metaData.CompareAndSetUUID(prevUUID, newUUID) {
			metaData.ResetSequence()
			metaData.ResetStaleSequence()
			// TODO:: log at trace level
			log.Println(fmt.Sprintf("%s:[name=%s,partition=%d,prevUuid=%s,newUuid=%s]",
				"Invalid UUID, lost remote partition data unexpectedly", r.name, partition, prevUUID, newUUID))
			break
		}
	}
}

func (r *RepairingHandler) CheckOrRepairSequence(partition int32, nextSeq int64, viaAntiEntropy bool) {
	metaData := r.MetaDataContainer(partition)
	for {
		currentSeq := metaData.Sequence()
		if currentSeq >= nextSeq {
			break
		}

		if metaData.CompareAndSetSequence(currentSeq, nextSeq) {
			seqDifference := nextSeq - currentSeq
			if viaAntiEntropy || seqDifference > 1 {
				var missCount int64
				if viaAntiEntropy {
					missCount = seqDifference
				} else {
					missCount = seqDifference - 1
				}
				totalMissCount := metaData.AddAndGetMissedSequenceCount(missCount)
				// TODO:: log at trace level
				log.Println(fmt.Sprintf("%s:[map=%s,partition=%d,currentSequence=%d,nextSequence=%d,totalMissCount=%d]",
					"Invalid sequence", r.name, partition, currentSeq, nextSeq, totalMissCount))
			}
			break
		}

	}
}

func (r *RepairingHandler) UpdateLastKnownStaleSequence(metaData *MetaDataContainer, partition int32) {
	for {
		lastReceivedSequence := metaData.Sequence()
		lastKnownStaleSequence := metaData.StaleSequence()
		if lastReceivedSequence >= lastKnownStaleSequence {
			break
		}
		if metaData.CompareAndSetStaleSequence(lastKnownStaleSequence, lastReceivedSequence) {
			break
		}
	}
	// TODO:: Log at trace level
	log.Println(fmt.Sprintf("%s:[map=%s,partition=%d,lowerSequencesStaleThan=%d,lastReceivedSequence=%d]",
		"Stale sequences updated", r.name, partition, metaData.StaleSequence(), metaData.Sequence()))
}

func (r *RepairingHandler) HandleSingleInvalidation(key serialization.Data, sourceUUID string,
	partitionUUID string, sequence int64) {
	if !r.isOriginatedFromLocalClient(sourceUUID) {
		if key == nil {
			r.nearCache.Clear()
		} else {
			if r.serializeKeys {
				r.nearCache.Invalidate(key)
			} else {
				keyObj, _ := r.serializationService.ToObject(key)
				r.nearCache.Invalidate(keyObj)
			}
		}
	}
	partitionID := r.GetPartitionIDOrDefault(key)
	r.CheckOrRepairUUID(partitionID, partitionUUID)
	r.CheckOrRepairSequence(partitionID, sequence, false)
}

func (r *RepairingHandler) isOriginatedFromLocalClient(sourceUUID string) bool {
	return r.localUUID == sourceUUID
}

func (r *RepairingHandler) GetPartitionIDOrDefault(key serialization.Data) int32 {
	if key == nil {
		// `name` is used to determine partition ID of map-wide events like clear()
		// since key is `nil`, we are using `name` to find the partition ID
		return r.partitionService.GetPartitionIDForObject(r.name)
	}
	return r.partitionService.GetPartitionID(key)
}
