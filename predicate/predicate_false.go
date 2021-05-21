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

import "github.com/hazelcast/hazelcast-go-client/serialization"

// False creates a predicate that always evaluates to false and passes no items.
func False() *predFalse {
	return &predFalse{}
}

type predFalse struct {
}

func (p predFalse) FactoryID() int32 {
	return factoryID
}

func (p predFalse) ClassID() int32 {
	return 13
}

func (p predFalse) ReadData(input serialization.DataInput) {
}

func (p predFalse) WriteData(output serialization.DataOutput) {
}

func (p predFalse) String() string {
	return "(false)"
}
