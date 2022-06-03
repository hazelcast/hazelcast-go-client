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

import (
	"fmt"

	pserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

type FieldKindOperation interface {
	KindSizeInBytes() int32
}

const VARIABLE_SIZE = -1

type Int32FieldKindOperation struct{}

func (Int32FieldKindOperation) KindSizeInBytes() int32 {
	return Int32SizeInBytes
}

type StringFieldKindOperation struct{}

func (StringFieldKindOperation) KindSizeInBytes() int32 {
	return VARIABLE_SIZE
}

func FieldOperations(fieldKind pserialization.FieldKind) FieldKindOperation {
	switch fieldKind {
	case pserialization.FieldKindInt32:
		return &Int32FieldKindOperation{}
	case pserialization.FieldKindString:
		return &StringFieldKindOperation{}
	default:
		panic(fmt.Sprintf("Unknown field kind for field operations: %d", fieldKind))
	}
}
