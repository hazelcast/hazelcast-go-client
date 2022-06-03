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

package serialization_test

import (
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type MainDTO struct {
	str *string
	i   int32
}

func NewMainDTO() MainDTO {
	str := "this is main object created for testing!"
	return MainDTO{
		i:   56789,
		str: &str,
	}
}

type MainDTOSerializer struct {
}

func (MainDTOSerializer) Type() reflect.Type {
	return reflect.TypeOf(MainDTO{})
}

func (MainDTOSerializer) TypeName() string {
	return "MainDTO"
}

func (MainDTOSerializer) Read(reader serialization.CompactReader) interface{} {
	return MainDTO{
		i:   reader.ReadInt32("i"),
		str: reader.ReadNullableString("str"),
	}
}

func (MainDTOSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	c, ok := value.(MainDTO)
	if !ok {
		panic("not a MainDTO")
	}
	writer.WriteInt32("i", c.i)
	writer.WriteNullableString("str", c.str)
}
