// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package core

import "github.com/hazelcast/hazelcast-go-client/v4/internal"

// Map is concurrent, distributed, observable and queryable.
// This map is sync (blocking). Blocking calls return the value of the call and block
// the execution until the return value is calculated.
// It does not allow nil to be used as a key or value.
type Map interface {
	// DistributedObject is the base interface for all distributed objects.
	internal.DistributedObject

	// Put returns the value with the specified key in this map.
	// If the map previously contained a mapping for
	// the key, the old value is replaced by the specified value.
	// Put returns a clone of the previous value, not the original (identically equal) value previously put
	// into the map.
	// Put returns nil if there was no mapping for the key.
	Put(key interface{}, value interface{}) (oldValue interface{}, err error)

	// Get returns the value for the specified key, or nil if this map does not contain this key.
	// Warning:
	// Get returns a clone of original value, modifying the returned value does not change the actual value in
	// the map. One should put modified value back to make changes visible to all nodes.
	//	value,_ = map.Get(key)
	//	value.updateSomeProperty()
	//	map.Put(key,value)
	Get(key interface{}) (value interface{}, err error)

	// Clear clears the map and deletes the items from the backing map store.
	Clear() (err error)
}
