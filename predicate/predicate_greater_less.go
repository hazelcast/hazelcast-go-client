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

type predGreaterLess struct {
	attribute string
	value     interface{}
	equal     bool
	less      bool
}

func (p predGreaterLess) FactoryID() int32 {
	return factoryID
}

func (p predGreaterLess) ClassID() int32 {
	return 4
}

func (p *predGreaterLess) ReadData(input serialization.DataInput) error {
	p.attribute = input.ReadString()
	p.value = input.ReadObject()
	p.equal = input.ReadBool()
	p.less = input.ReadBool()
	return input.Error()
}

func (p predGreaterLess) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.attribute)
	if err := output.WriteObject(p.value); err != nil {
		return err
	}
	output.WriteBool(p.equal)
	output.WriteBool(p.less)
	return nil
}

func (p predGreaterLess) String() string {
	return fmt.Sprintf("%s>=%v", p.attribute, p.value)
}
