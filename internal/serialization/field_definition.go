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

import pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"

func NewFieldDefinition(index int32, fieldName string, fieldType pubserialization.FieldDefinitionType, factoryID int32,
	classID int32, version int32) pubserialization.FieldDefinition {
	return pubserialization.FieldDefinition{
		Index:     index,
		Name:      fieldName,
		Type:      fieldType,
		FactoryID: factoryID,
		ClassID:   classID,
		Version:   version,
	}
}
