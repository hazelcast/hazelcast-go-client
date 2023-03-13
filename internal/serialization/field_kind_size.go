/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import (
	pserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	variableKindSize = -1
	invalidKindSize  = -2
)

var FieldKindSize = [pserialization.FieldKindArrayOfNullableFloat64 + 1]int32{}

func init() {
	for i := int32(2); i <= int32(pserialization.FieldKindArrayOfNullableFloat64); i++ {
		FieldKindSize[i] = variableKindSize
	}
	FieldKindSize[pserialization.FieldKindNotAvailable] = invalidKindSize // 0
	FieldKindSize[pserialization.FieldKindBoolean] = 0                    // 1
	FieldKindSize[pserialization.FieldKindInt8] = ByteSizeInBytes         // 3
	FieldKindSize[pserialization.FieldKindChar] = 0                       // 5
	FieldKindSize[pserialization.FieldKindInt16] = Int16SizeInBytes       // 7
	FieldKindSize[pserialization.FieldKindInt32] = Int32SizeInBytes       // 9
	FieldKindSize[pserialization.FieldKindInt64] = Int64SizeInBytes       // 11
	FieldKindSize[pserialization.FieldKindFloat32] = Float32SizeInBytes   // 13
	FieldKindSize[pserialization.FieldKindFloat64] = Float64SizeInBytes   // 15
}
