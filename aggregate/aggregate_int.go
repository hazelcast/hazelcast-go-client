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

// IntAverage returns the average of values of the given attribute.
// Note that this function may not work as expected for Hazelcast versions prior to 5.0.
func IntAverage(attr string) *aggIntAverage {
	return &aggIntAverage{attrPath: attr}
}

// IntAverageAll returns the average of all values of the given attribute.
func IntAverageAll() *aggIntAverage {
	return &aggIntAverage{attrPath: ""}
}

// IntSum returns the sum of values of the given attribute.
// Note that this function may not work as expected for Hazelcast versions prior to 5.0.
func IntSum(attr string) *aggIntSum {
	return &aggIntSum{attrPath: attr}
}

// IntSumAll returns the sum of all values of the given attribute.
func IntSumAll() *aggIntSum {
	return &aggIntSum{attrPath: ""}
}

type aggIntAverage struct {
	attrPath string
}

func (a aggIntAverage) FactoryID() int32 {
	return internal.AggregateFactoryID
}

func (a aggIntAverage) ClassID() (classID int32) {
	return 10
}

func (a aggIntAverage) WriteData(output serialization.DataOutput) {
	writeAttrPath(output, a.attrPath)
	// member side, not used in client
	output.WriteInt64(0)
	output.WriteInt64(0)
}

func (a *aggIntAverage) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side, not used in client
	input.ReadInt64()
	input.ReadInt64()
}

func (a aggIntAverage) String() string {
	return makeString("IntAverage", a.attrPath)
}

type aggIntSum struct {
	attrPath string
}

func (a aggIntSum) FactoryID() int32 {
	return internal.AggregateFactoryID
}

func (a aggIntSum) ClassID() (classID int32) {
	return 11
}

func (a aggIntSum) WriteData(output serialization.DataOutput) {
	writeAttrPath(output, a.attrPath)
	// member side, not used in client
	output.WriteInt64(0)
}

func (a *aggIntSum) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side, not used in client
	input.ReadInt64()
}

func (a aggIntSum) String() string {
	return makeString("IntSum", a.attrPath)
}
