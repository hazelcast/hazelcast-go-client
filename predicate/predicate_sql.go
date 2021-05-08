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
SQL creates a predicate that will pass items that match the given SQL where expression.

The following operators are supported: =, <, >, <=, >=, ==, !=, <>,
BETWEEN, IN, LIKE, ILIKE, REGEX, AND, OR, NOT.

The operators are case-insensitive, but attribute names are case sensitive.

Example:

	active AND (age > 20 OR salary < 60000)

Differences to standard SQL:
* We don't use ternary boolean logic. field=10 evaluates to false, if field is null, in standard SQL it evaluates to UNKNOWN.
* IS [NOT] NULL is not supported, use =NULL or <>NULL.
* IS [NOT] DISTINCT FROM is not supported, but = and <> behave like it.
*/
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

func (p *predSQL) ReadData(input serialization.DataInput) {
	p.expression = input.ReadString()
}

func (p predSQL) WriteData(output serialization.DataOutput) {
	output.WriteString(p.expression)
}

func (p predSQL) String() string {
	return fmt.Sprintf("SQL(%s)", p.expression)
}
