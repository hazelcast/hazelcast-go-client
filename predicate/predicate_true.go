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

// True creates a predicate that always evaluates to true and passes all items.
func True() *predTrue {
	return &predTrue{}
}

type predTrue struct {
}

func (p predTrue) FactoryID() int32 {
	return factoryID
}

func (p predTrue) ClassID() int32 {
	return 14
}

func (p predTrue) ReadData(input serialization.DataInput) {
}

func (p predTrue) WriteData(output serialization.DataOutput) {
}

func (p predTrue) String() string {
	return "(false)"
}
