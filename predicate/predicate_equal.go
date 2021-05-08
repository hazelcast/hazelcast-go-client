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

	serialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

// Equal creates a predicate that will pass items if the given value and the value stored under the given item attribute are equal.
func Equal(attributeName string, value interface{}) *predEqual {
	return &predEqual{
		attribute: attributeName,
		value:     value,
	}
}

type predEqual struct {
	attribute string
	value     interface{}
}

func (p predEqual) FactoryID() int32 {
	return factoryID
}

func (p predEqual) ClassID() int32 {
	return 3
}

func (p *predEqual) ReadData(input serialization.DataInput) {
	p.attribute = input.ReadString()
	p.value = input.ReadObject()
}

func (p predEqual) WriteData(output serialization.DataOutput) {
	output.WriteString(p.attribute)
	output.WriteObject(p.value)
}

func (p predEqual) String() string {
	return fmt.Sprintf("%s=%v", p.attribute, p.value)
}
