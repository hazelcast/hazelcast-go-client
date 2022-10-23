/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package serialization

const rabinFingerPrintInit int64 = -4513414715797952619

type RabinFingerPrint struct {
	table [256]int64
}

func NewRabinFingerPrint() RabinFingerPrint {
	r := RabinFingerPrint{}

	for i := int64(0); i < 256; i++ {
		fp := i
		for j := 0; j < 8; j++ {
			fp = int64(uint64(fp)>>1) ^ (rabinFingerPrintInit & -(fp & 1))
		}
		r.table[i] = fp
	}
	return r
}

func (r RabinFingerPrint) OfSchema(schema *Schema) int64 {
	fp := r.ofString(rabinFingerPrintInit, schema.TypeName)
	fp = r.ofInt32(fp, int32(schema.FieldCount()))
	for _, descriptor := range schema.fieldDefinitions {
		fp = r.ofString(fp, descriptor.FieldName)
		fp = r.ofInt32(fp, int32(descriptor.Kind))
	}
	return fp
}

func (r RabinFingerPrint) ofString(f int64, value string) int64 {
	bytes := []byte(value)
	fp := r.ofInt32(f, int32(len(bytes)))
	for _, b := range bytes {
		fp = r.ofByte(fp, b)
	}
	return fp
}

func (r RabinFingerPrint) ofInt32(f int64, value int32) int64 {
	fp := r.ofByte(f, byte(value&0xff))
	fp = r.ofByte(fp, byte((value>>8)&0xff))
	fp = r.ofByte(fp, byte((value>>16)&0xff))
	fp = r.ofByte(fp, byte((value>>24)&0xff))
	return fp
}

func (r RabinFingerPrint) ofByte(f int64, value byte) int64 {
	var rightShifted int64
	if f >= 0 {
		rightShifted = f >> 8
	} else {
		rightShifted = int64(uint64(f) >> 8)
	}
	return rightShifted ^ r.table[(f^int64(value))&0xff]
}
