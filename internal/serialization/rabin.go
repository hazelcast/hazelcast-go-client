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

package serialization

const INIT int64 = -4513414715797952619

type RabinFingerPrint struct {
	table []int64
}

func NewRabinFingerPrint() RabinFingerPrint {
	return RabinFingerPrint{
		table: make([]int64, 256),
	}
}

func (r RabinFingerPrint) Init() {
	for i := 0; i < 256; i++ {
		fp := int64(i)
		for j := 0; j < 8; j++ {
			fp = (int64(uint64(fp)>>1) ^ (INIT & -(fp & 1)))
		}
		r.table[i] = fp
	}
}

func (r RabinFingerPrint) OfSchema(schema Schema) int64 {
	fingerprint := r.ofString(INIT, schema.TypeName())
	fingerprint = r.ofInt32(fingerprint, int32(schema.FieldCount()))
	for _, descriptor := range schema.FieldDefinitionMap() {
		fingerprint = r.ofString(fingerprint, descriptor.fieldName)
		fingerprint = r.ofInt32(fingerprint, int32(descriptor.fieldKind))
	}
	return fingerprint
}

func (r RabinFingerPrint) ofString(fp int64, value string) int64 {
	
}

func (r RabinFingerPrint) ofInt32(fp int64, value int32) int64 {

}
