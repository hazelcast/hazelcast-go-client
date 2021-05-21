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

	serialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

/*
And creates a predicate that will perform the logical and operation on the given predicates.
If no predicate is provided as argument, the created predicate will always evaluate to true.
*/
func And(predicates ...Predicate) *predAnd {
	return &predAnd{predicates: predicates}
}

type predAnd struct {
	predicates []Predicate
}

func (p predAnd) FactoryID() int32 {
	return factoryID
}

func (p predAnd) ClassID() int32 {
	return 1
}

func (p *predAnd) ReadData(input serialization.DataInput) {
	length := input.ReadInt32()
	predicates := make([]Predicate, length)
	for i := 0; i < int(length); i++ {
		pred := input.ReadObject()
		predicates[i] = pred.(Predicate)
	}
	p.predicates = predicates
}

func (p predAnd) WriteData(output serialization.DataOutput) {
	output.WriteInt32(int32(len(p.predicates)))
	for _, pred := range p.predicates {
		output.WriteObject(pred)
	}
}

func (p predAnd) String() string {
	ps := make([]string, len(p.predicates))
	for i, pr := range p.predicates {
		ps[i] = pr.String()
	}
	return fmt.Sprintf("And(%s)", strings.Join(ps, ", "))
}
