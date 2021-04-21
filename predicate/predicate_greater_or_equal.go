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

func GreaterOrEqual(attributeName string, value interface{}) *predGreaterOrEqual {
	return &predGreaterOrEqual{
		attribute: attributeName,
		value:     value,
	}
}

type predGreaterOrEqual struct {
	attribute string
	value     interface{}
}

func (p predGreaterOrEqual) FactoryID() int32 {
	return factoryID
}

func (p predGreaterOrEqual) ClassID() int32 {
	return 4
}

func (p *predGreaterOrEqual) ReadData(input serialization.DataInput) error {
	p.attribute = input.ReadString()
	p.value = input.ReadObject()
	return input.Error()
}

func (p predGreaterOrEqual) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.attribute)
	return output.WriteObject(p.value)
}

func (p predGreaterOrEqual) String() string {
	return fmt.Sprintf("%s>=%v", p.attribute, p.value)
}

func (p predGreaterOrEqual) enforcePredicate() {

}
