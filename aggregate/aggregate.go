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

package aggregate

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const nilArrayLength = -1

type Aggregator interface {
	serialization.IdentifiedDataSerializable
	fmt.Stringer
}

func writeAttrPath(output serialization.DataOutput, attrPath string) {
	if attrPath == "" {
		output.WriteInt32(nilArrayLength)
	} else {
		output.WriteString(attrPath)
	}
}

func makeString(name, attrPath string) string {
	if attrPath == "" {
		return fmt.Sprintf("%s()", name)
	}
	return fmt.Sprintf("%s(%s)", name, attrPath)
}
