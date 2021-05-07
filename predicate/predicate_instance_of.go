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

// InstanceOf creates a predicate that will pass entries for which the value class is an instance of the given className.
func InstanceOf(className string) *predInstanceOf {
	return &predInstanceOf{
		className: className,
	}
}

type predInstanceOf struct {
	className string
}

func (p predInstanceOf) FactoryID() int32 {
	return factoryID
}

func (p predInstanceOf) ClassID() int32 {
	return 8
}

func (p *predInstanceOf) ReadData(input serialization.DataInput) error {
	p.className = input.ReadString()
	return input.Error()
}

func (p predInstanceOf) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.className)
	return nil
}

func (p predInstanceOf) String() string {
	return fmt.Sprintf("InstanceOf(%s)", p.className)
}
