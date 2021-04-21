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

func Not(predicate Predicate) *predNot {
	return &predNot{
		pred: predicate,
	}
}

type predNot struct {
	pred Predicate
}

func (p predNot) FactoryID() int32 {
	return factoryID
}

func (p predNot) ClassID() int32 {
	return 10
}

func (p *predNot) ReadData(input serialization.DataInput) error {
	p.pred = input.ReadObject().(Predicate)
	return input.Error()
}

func (p predNot) WriteData(output serialization.DataOutput) error {
	return output.WriteObject(p.pred)
}

func (p predNot) String() string {
	return fmt.Sprintf("~%v", p.pred)
}

func (p predNot) enforcePredicate() {

}
