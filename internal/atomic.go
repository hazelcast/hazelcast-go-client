/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package internal

import (
	"sync/atomic"
	"unsafe"
)

// AtomicValue keeps the pointer to an interface{}.
// Storing and loading values is concurrency-safe.
type AtomicValue struct {
	value unsafe.Pointer
}

// Store stores the given interface{} pointer.
func (a *AtomicValue) Store(v *interface{}) {
	atomic.StorePointer(&a.value, unsafe.Pointer(v))
}

// Load loads the given interface{} pointer and returns an interface{}.
func (a *AtomicValue) Load() interface{} {
	v := (*interface{})(atomic.LoadPointer(&a.value))
	if v == nil {
		return v
	}
	return *v
}
