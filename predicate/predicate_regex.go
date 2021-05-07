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
Regex creates a predicate that will pass items if the given pattern matches the value stored under the given item attribute.

The pattern interpreted exactly the same as described in https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html.
*/
func Regex(attributeName string, expression string) *predRegex {
	return &predRegex{
		attribute:  attributeName,
		expression: expression,
	}
}

type predRegex struct {
	attribute  string
	expression string
}

func (p predRegex) FactoryID() int32 {
	return factoryID
}

func (p predRegex) ClassID() int32 {
	return 12
}

func (p *predRegex) ReadData(input serialization.DataInput) error {
	p.attribute = input.ReadString()
	p.expression = input.ReadString()
	return input.Error()
}

func (p predRegex) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.attribute)
	output.WriteString(p.expression)
	return nil
}

func (p predRegex) String() string {
	return fmt.Sprintf("Regex(%s, %s)", p.attribute, p.expression)
}
