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

func SQL(expression string) *predSQL {
	return &predSQL{
		expression: expression,
	}
}

type predSQL struct {
	expression string
}

func (p predSQL) FactoryID() int32 {
	return factoryID
}

func (p predSQL) ClassID() int32 {
	return 0
}

func (p *predSQL) ReadData(input serialization.DataInput) error {
	p.expression = input.ReadString()
	return input.Error()
}

func (p predSQL) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.expression)
	return nil
}

func (p predSQL) String() string {
	return fmt.Sprintf("SQL(%s)", p.expression)
}

func (p predSQL) enforcePredicate() {

}
