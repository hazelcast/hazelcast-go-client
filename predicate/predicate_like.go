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

func Like(attributeName string, expression string) *predLike {
	return &predLike{
		attribute:  attributeName,
		expression: expression,
	}
}

type predLike struct {
	attribute  string
	expression string
}

func (p predLike) FactoryID() int32 {
	return factoryID
}

func (p predLike) ClassID() int32 {
	return 5
}

func (p *predLike) ReadData(input serialization.DataInput) error {
	p.attribute = input.ReadString()
	p.expression = input.ReadString()
	return input.Error()
}

func (p predLike) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.attribute)
	output.WriteString(p.expression)
	return nil
}

func (p predLike) String() string {
	return fmt.Sprintf("Like(%s, %s)", p.attribute, p.expression)
}

func (p predLike) enforcePredicate() {

}
