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

package internal

import "github.com/hazelcast/hazelcast-go-client/serialization"

type EmptyObjectDataOutput struct {
	*PositionalObjectDataOutput
}

func NewEmptyObjectDataOutput() *EmptyObjectDataOutput {
	return &EmptyObjectDataOutput{}
}

func (o *EmptyObjectDataOutput) WriteByte(v byte) {
}

func (o *EmptyObjectDataOutput) WriteBool(v bool) {
}

func (o *EmptyObjectDataOutput) WriteUInt16(v uint16) {
}

func (o *EmptyObjectDataOutput) WriteInt16(v int16) {
}

func (o *EmptyObjectDataOutput) WriteInt32(v int32) {
}

func (o *EmptyObjectDataOutput) WriteInt64(v int64) {
}

func (o *EmptyObjectDataOutput) WriteFloat32(v float32) {
}

func (o *EmptyObjectDataOutput) WriteFloat64(v float64) {
}

func (o *EmptyObjectDataOutput) WriteUTF(v string) {
}

func (o *EmptyObjectDataOutput) WriteObject(object interface{}) error {
	return nil
}

func (o *EmptyObjectDataOutput) WriteByteArray(v []byte) {
}

func (o *EmptyObjectDataOutput) WriteBoolArray(v []bool) {
}

func (o *EmptyObjectDataOutput) WriteUInt16Array(v []uint16) {
}

func (o *EmptyObjectDataOutput) WriteInt16Array(v []int16) {
}

func (o *EmptyObjectDataOutput) WriteInt32Array(v []int32) {
}

func (o *EmptyObjectDataOutput) WriteInt64Array(v []int64) {
}

func (o *EmptyObjectDataOutput) WriteFloat32Array(v []float32) {
}

func (o *EmptyObjectDataOutput) WriteFloat64Array(v []float64) {
}

func (o *EmptyObjectDataOutput) WriteUTFArray(v []string) {
}

func (o *EmptyObjectDataOutput) WriteBytes(v string) {
}

func (o *EmptyObjectDataOutput) WriteData(data serialization.Data) {
}
