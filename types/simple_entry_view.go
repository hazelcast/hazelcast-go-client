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

package types

// SimpleEntryView represents a readonly view of a map entry.
type SimpleEntryView struct {
	// TODO: export fields
	Key            interface{}
	Value          interface{}
	Cost           int64
	CreationTime   int64
	ExpirationTime int64
	Hits           int64
	LastAccessTime int64
	LastStoredTime int64
	LastUpdateTime int64
	Version        int64
	TTL            int64
	MaxIdle        int64
}

func NewSimpleEntryView(key, value interface{}, cost, creationTime, expirationTime, hits, lastAccessTime,
	lastStoredTime, lastUpdateTime, version, ttl, maxIdle int64) *SimpleEntryView {
	return &SimpleEntryView{key, value, cost, creationTime, expirationTime,
		hits, lastAccessTime, lastStoredTime, lastUpdateTime,
		version, ttl, maxIdle}
}
