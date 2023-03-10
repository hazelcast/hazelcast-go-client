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

import pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"

type FieldDescriptor struct {
	Name      string
	Kind      pubserialization.FieldKind
	index     int32
	offset    int32
	bitOffset int8
}

func NewFieldDescriptor(fieldName string, fieldKind pubserialization.FieldKind) FieldDescriptor {
	return FieldDescriptor{
		Name:      fieldName,
		Kind:      fieldKind,
		index:     -1,
		offset:    -1,
		bitOffset: -1,
	}
}
