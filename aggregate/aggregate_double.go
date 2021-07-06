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

// DoubleAverage returns the average of values of the given attribute.
// Note that this function may not work as expected for Hazelcast versions prior to 5.0.
func DoubleAverage(attr string) *aggDoubleAverage {
	return &aggDoubleAverage{attrPath: attr}
}

// DoubleAverageAll returns the average of all values of the given attribute.
func DoubleAverageAll() *aggDoubleAverage {
	return &aggDoubleAverage{attrPath: ""}
}

// DoubleSum returns the sum of values of the given attribute.
// Note that this function may not work as expected for Hazelcast versions prior to 5.0.
func DoubleSum(attr string) *aggDoubleSum {
	return &aggDoubleSum{attrPath: attr}
}

// DoubleSumAll returns the sum of all values of the given attribute.
func DoubleSumAll() *aggDoubleSum {
	return &aggDoubleSum{attrPath: ""}
}

type aggDoubleAverage struct {
	attrPath string
}

func (a aggDoubleAverage) FactoryID() int32 {
	return internal.AggregateFactoryID
}

func (a aggDoubleAverage) ClassID() (classID int32) {
	return 6
}

func (a aggDoubleAverage) WriteData(output serialization.DataOutput) {
	writeAttrPath(output, a.attrPath)
	// member side, not used in client
	output.WriteFloat64(0)
	output.WriteInt64(0)
}

func (a *aggDoubleAverage) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side, not used in client
	input.ReadFloat64()
	input.ReadInt64()
}

func (a aggDoubleAverage) String() string {
	return makeString("DoubleAverage", a.attrPath)
}

type aggDoubleSum struct {
	attrPath string
}

func (a aggDoubleSum) FactoryID() int32 {
	return internal.AggregateFactoryID
}

func (a aggDoubleSum) ClassID() (classID int32) {
	return 7
}

func (a aggDoubleSum) WriteData(output serialization.DataOutput) {
	writeAttrPath(output, a.attrPath)
	// member side, not used in client
	output.WriteFloat64(0)
}

func (a *aggDoubleSum) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side, not used in client
	input.ReadFloat64()
}

func (a aggDoubleSum) String() string {
	return makeString("DoubleSum", a.attrPath)
}
