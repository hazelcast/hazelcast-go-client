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

import "github.com/hazelcast/hazelcast-go-client/internal/nearcache"

type StaleReadDetector interface {
	IsStaleRead(key interface{}, record nearcache.Record) bool
	PartitionID(key interface{}) int32
	MetaDataContainer(partitionID int32) *MetaDataContainer
}

var AlwaysFresh = &alwaysFresh{}

type alwaysFresh struct {
}

func (a *alwaysFresh) IsStaleRead(key interface{}, record nearcache.Record) bool {
	return false
}

func (a *alwaysFresh) PartitionID(key interface{}) int32 {
	return 0
}

func (a *alwaysFresh) MetaDataContainer(partitionID int32) *MetaDataContainer {
	return nil
}
