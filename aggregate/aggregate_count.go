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

package aggregate

import (
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

// Count returns the number of values which includes the given attribute.
// Note that this function may not work as expected for Hazelcast versions prior to 5.0.
func Count(attr string) *aggCount {
	return &aggCount{attrPath: attr}
}

// CountAll returns the number of all values.
func CountAll() *aggCount {
	return &aggCount{attrPath: ""}
}

type aggCount struct {
	attrPath string
}

func (a aggCount) FactoryID() int32 {
	return internal.AggregateFactoryID
}

func (a aggCount) ClassID() (classID int32) {
	return 4
}

func (a aggCount) WriteData(output serialization.DataOutput) {
	writeAttrPath(output, a.attrPath)
	// member side field, not used in client
	output.WriteInt64(0)
}

func (a *aggCount) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side field, not used in client
	input.ReadInt64()
}

func (a aggCount) String() string {
	return makeString("Count", a.attrPath)
}
