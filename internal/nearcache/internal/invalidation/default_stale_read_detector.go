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
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type DefaultStaleReadDetector struct {
	repairingHandler *RepairingHandler
	partitionService *internal.PartitionService
}

func (d *DefaultStaleReadDetector) IsStaleRead(key interface{}, record nearcache.Record) bool {
	latestMetadata := d.repairingHandler.MetaDataContainer(record.PartitionID())
	return !record.HasSameUUID(latestMetadata.UUID()) || record.InvalidationSequence() < latestMetadata.StaleSequence()
}

func (d *DefaultStaleReadDetector) PartitionID(keyData serialization.Data) int32 {
	return d.partitionService.GetPartitionID(keyData)
}

func (d *DefaultStaleReadDetector) MetaDataContainer(partitionID int32) *MetaDataContainer {
	return d.repairingHandler.MetaDataContainer(partitionID)
}
