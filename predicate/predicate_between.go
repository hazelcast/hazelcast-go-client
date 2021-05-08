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

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

/*
Between creates a predicate that will pass items if the value stored under the given item attribute is contained inside the given range.

The bounds are inclusive.
*/
func Between(attribute string, from interface{}, to interface{}) *predBetween {
	return &predBetween{
		attribute: attribute,
		from:      from,
		to:        to,
	}
}

type predBetween struct {
	attribute string
	from      interface{}
	to        interface{}
}

func (p predBetween) FactoryID() int32 {
	return factoryID
}

func (p predBetween) ClassID() int32 {
	return 2
}

func (p *predBetween) ReadData(input serialization.DataInput) {
	p.attribute = input.ReadString()
	p.to = input.ReadObject()
	p.from = input.ReadObject()
}

func (p predBetween) WriteData(output serialization.DataOutput) {
	output.WriteString(p.attribute)
	output.WriteObject(p.to)
	output.WriteObject(p.from)
}

func (p predBetween) String() string {
	return fmt.Sprintf("Between('%s', %#v, %#v)", p.attribute, p.from, p.to)
}
