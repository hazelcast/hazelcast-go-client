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

// Min returns the minimum of the values corresponding to the given attribute.
func Min(attr string) *aggMin {
	return &aggMin{attrPath: attr}
}

// MinAll returns the minimum of all values.
func MinAll() *aggMin {
	return &aggMin{attrPath: ""}
}

// Max returns the maximum of the values corresponding to the given attribute.
func Max(attr string) *aggMax {
	return &aggMax{attrPath: attr}
}

// MaxAll returns the maximum of all values.
func MaxAll() *aggMax {
	return &aggMax{attrPath: ""}
}

type aggMin struct {
	attrPath string
}

func (a aggMin) FactoryID() int32 {
	return internal.AggregateFactoryID
}

func (a aggMin) ClassID() (classID int32) {
	return 15
}

func (a aggMin) WriteData(output serialization.DataOutput) {
	writeAttrPath(output, a.attrPath)
	// member side, not used in client
	output.WriteObject(nil)
}

func (a *aggMin) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side, not used in client
	input.ReadObject()
}

func (a aggMin) String() string {
	return makeString("Min", a.attrPath)
}

type aggMinBy struct {
	attrPath string
}

func (a aggMinBy) FactoryID() int32 {
	return internal.AggregateFactoryID
}

func (a aggMinBy) ClassID() (classID int32) {
	return 18
}

func (a aggMinBy) WriteData(output serialization.DataOutput) {
	writeAttrPath(output, a.attrPath)
	// member side, not used in client
	output.WriteObject(nil)
}

func (a *aggMinBy) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side, not used in client
	input.ReadObject()
}

func (a aggMinBy) String() string {
	return makeString("MinBy", a.attrPath)
}

type aggMax struct {
	attrPath string
}

func (a aggMax) FactoryID() int32 {
	return internal.AggregateFactoryID
}

func (a aggMax) ClassID() (classID int32) {
	return 14
}

func (a aggMax) WriteData(output serialization.DataOutput) {
	writeAttrPath(output, a.attrPath)
	// member side, not used in client
	output.WriteObject(nil)
}

func (a *aggMax) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side, not used in client
	input.ReadObject()
}

func (a aggMax) String() string {
	return makeString("Max", a.attrPath)
}
