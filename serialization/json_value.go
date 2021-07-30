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

package serialization

import ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"

type JSON []byte

func (j JSON) String() string {
	return string(j)
}

// MarshalJSON is used by functions in the Go `encoding/json` package.
// This function ensures that a `serialization.JSON` value is correctly
// serialized by this package. This is mostly applicable when
// `serialization.JSON` is serialized as a field of a larger struct.
func (j JSON) MarshalJSON() ([]byte, error) {
	if j == nil {
		return []byte("null"), nil
	}
	return j, nil
}

// UnmarshalJSON is used by functions in the Go `encoding/json` package.
// This function ensures that a `serialization.JSON` value is correctly
// deserialized from JSON objects that contain a field of this type.
func (j *JSON) UnmarshalJSON(data []byte) error {
	if j == nil {
		return ihzerrors.NewIllegalArgumentError("serialization.JSON: UnmarshalJSON on nil pointer", nil)
	}
	*j = append((*j)[0:0], data...)
	return nil
}
