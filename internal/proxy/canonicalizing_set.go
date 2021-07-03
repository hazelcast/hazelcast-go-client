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

package proxy

import (
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const canonicalizingSetClassID = 19

type AggregateFactory struct {
}

func (a AggregateFactory) Create(id int32) serialization.IdentifiedDataSerializable {
	if id == canonicalizingSetClassID {
		return &AggCanonicalizingSet{}
	}
	return nil
}

func (a AggregateFactory) FactoryID() int32 {
	return internal.AggregateFactoryID
}

type AggCanonicalizingSet []interface{}

func (a AggCanonicalizingSet) FactoryID() int32 {
	return internal.AggregateFactoryID
}

func (a AggCanonicalizingSet) ClassID() (classID int32) {
	return canonicalizingSetClassID
}

func (a AggCanonicalizingSet) WriteData(output serialization.DataOutput) {
	output.WriteInt32(int32(len(a)))
	for k := range a {
		output.WriteObject(k)
	}
}

func (a *AggCanonicalizingSet) ReadData(input serialization.DataInput) {
	size := int(input.ReadInt32())
	values := make([]interface{}, size)
	for i := 0; i < size; i++ {
		values[i] = input.ReadObject()
	}
	*a = values
}
