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

package predicate

import (
	"fmt"
	"strings"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

// In creates a predicate that will pass items if the value stored under the given item attribute is a member of the given values.
func In(attributeName string, values ...interface{}) *predIn {
	return &predIn{
		attribute: attributeName,
		values:    values,
	}
}

type predIn struct {
	attribute string
	values    []interface{}
}

func (p predIn) FactoryID() int32 {
	return factoryID
}

func (p predIn) ClassID() int32 {
	return 7
}

func (p *predIn) ReadData(input serialization.DataInput) {
	p.attribute = input.ReadString()
	numValues := int(input.ReadInt32())
	values := make([]interface{}, numValues)
	for i := 0; i < numValues; i++ {
		values[i] = input.ReadObject()
	}
	p.values = values
}

func (p predIn) WriteData(output serialization.DataOutput) {
	output.WriteString(p.attribute)
	output.WriteInt32(int32(len(p.values)))
	for _, value := range p.values {
		output.WriteObject(value)
	}
}

func (p predIn) String() string {
	vs := make([]string, len(p.values))
	for i, value := range p.values {
		vs[i] = fmt.Sprintf("%#v", value)
	}
	return fmt.Sprintf("In(%s, %s)", p.attribute, strings.Join(vs, ", "))
}
