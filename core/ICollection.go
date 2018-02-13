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

package core

// ICollection is concurrent, distributed, partitioned, listenable collection.
type ICollection interface {

	// AddItemListener adds an item listener for this collection.
	// This listener will be invoked whenever an item is added to or removed from this collection.
	// AddItemListener returns registrationId of the listener.
	AddItemListener(listener interface{}, includeValue bool) (registrationID *string, err error)

	// RemoveItemListener removes the item listener with the given registrationId.
	// RemoveItemListener returns true if the listener is removed, false otherwise.
	RemoveItemListener(registrationId *string) (removed bool, err error)

	// Size returns the number of elements in this collection.
	Size() (size int32, err error)

	// ToSlice returns a slice that contains all elements of this collection in proper sequence.
	ToSlice() (elements []interface{}, err error)
}
