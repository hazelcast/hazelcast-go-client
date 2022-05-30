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

type FieldDescriptor struct {
	fieldName string
	fieldKind FieldKind
	index     int32
	offset    int32
	bitOffset int8
}

func NewFieldDescriptor(fieldName string, fieldKind FieldKind) FieldDescriptor {
	return FieldDescriptor{
		fieldName: fieldName,
		fieldKind: fieldKind,
		index:     -1,
		offset:    -1,
		bitOffset: -1,
	}
}

type FieldKind int32

const (
	FieldKindInt32  FieldKind = 8
	FieldKindString FieldKind = 16
)
