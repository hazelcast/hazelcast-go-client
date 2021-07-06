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

// LongAverage returns the average of values of the given attribute.
// Note that this function may not work as expected for Hazelcast versions prior to 5.0.
func LongAverage(attr string) *aggLongAverage {
	return &aggLongAverage{attrPath: attr}
}

// LongAverageAll returns the average of all values of the given attribute.
func LongAverageAll() *aggLongAverage {
	return &aggLongAverage{attrPath: ""}
}

// LongSum returns the sum of values of the given attribute.
// Note that this function may not work as expected for Hazelcast versions prior to 5.0.
func LongSum(attr string) *aggLongSum {
	return &aggLongSum{attrPath: attr}
}

// LongSumAll returns the sum of all values of the given attribute.
func LongSumAll() *aggLongSum {
	return &aggLongSum{attrPath: ""}
}

type aggLongAverage struct {
	attrPath string
}

func (a aggLongAverage) FactoryID() int32 {
	return internal.AggregateFactoryID
}

func (a aggLongAverage) ClassID() (classID int32) {
	return 12
}

func (a aggLongAverage) WriteData(output serialization.DataOutput) {
	writeAttrPath(output, a.attrPath)
	// member side, not used in client
	output.WriteInt64(0)
	output.WriteInt64(0)
}

func (a *aggLongAverage) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side, not used in client
	input.ReadInt64()
	input.ReadInt64()
}

func (a aggLongAverage) String() string {
	return makeString("LongAverage", a.attrPath)
}

type aggLongSum struct {
	attrPath string
}

func (a aggLongSum) FactoryID() int32 {
	return internal.AggregateFactoryID
}

func (a aggLongSum) ClassID() (classID int32) {
	return 13
}

func (a aggLongSum) WriteData(output serialization.DataOutput) {
	writeAttrPath(output, a.attrPath)
	// member side, not used in client
	output.WriteInt64(0)
}

func (a *aggLongSum) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side, not used in client
	input.ReadInt64()
}

func (a aggLongSum) String() string {
	return makeString("LongSum", a.attrPath)
}
