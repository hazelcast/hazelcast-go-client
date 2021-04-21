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

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func Or(predicates ...Predicate) *predOr {
	return &predOr{predicates: predicates}
}

type predOr struct {
	predicates []Predicate
}

func (p predOr) FactoryID() int32 {
	return factoryID
}

func (p predOr) ClassID() int32 {
	return 11
}

func (p *predOr) ReadData(input serialization.DataInput) error {
	length := input.ReadInt32()
	predicates := make([]Predicate, length)
	for i := 0; i < int(length); i++ {
		pred := input.ReadObject()
		predicates[i] = pred.(Predicate)
	}
	p.predicates = predicates
	return input.Error()
}

func (p predOr) WriteData(output serialization.DataOutput) error {
	output.WriteInt32(int32(len(p.predicates)))
	for _, pred := range p.predicates {
		err := output.WriteObject(pred)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p predOr) String() string {
	ps := make([]string, len(p.predicates))
	for i, pr := range p.predicates {
		ps[i] = pr.String()
	}
	return fmt.Sprintf("Or(%s)", strings.Join(ps, ", "))
}

func (p predOr) enforcePredicate() {

}
