/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nearcache

import (
	icluster "github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type StaleReadDetector struct {
	rh *RepairingHandler
	ps *icluster.PartitionService
}

func NewStaleReadDetector(rh *RepairingHandler, ps *icluster.PartitionService) StaleReadDetector {
	return StaleReadDetector{
		rh: rh,
		ps: ps,
	}
}

func (sr StaleReadDetector) IsStaleRead(rec *Record) bool {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetectorImpl#isStaleRead
	// key param in the original implementation is not used.
	md := sr.rh.GetMetaDataContainer(rec.PartitionID())
	return !hasSameUUID(rec.UUID(), md.UUID()) || rec.InvalidationSequence() < md.StaleSequence()
}

func (sr StaleReadDetector) GetPartitionID(keyData serialization.Data) (int32, error) {
	return sr.ps.GetPartitionID(keyData)
}

func (sr StaleReadDetector) GetMetaDataContainer(partitionID int32) *MetaDataContainer {
	return sr.rh.GetMetaDataContainer(partitionID)
}

func hasSameUUID(u1, u2 types.UUID) bool {
	return !u1.Default() && !u2.Default() && u1 == u2
}
