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

package validationutil

import (
	"fmt"
	"math"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
)

const (
	nonNegativeValueExpected = "non-negative integer number expected: %d"
	int32ValueExpected       = "signed 32-bit integer number expected: %d"
)

func ValidateAsNonNegativeInt32(n int) (int32, error) {
	if n < 0 {
		return 0, hzerrors.NewHazelcastIllegalArgumentError(fmt.Sprintf(nonNegativeValueExpected, n), nil)
	}
	if n > math.MaxInt32 {
		return 0, hzerrors.NewHazelcastIllegalArgumentError(fmt.Sprintf(int32ValueExpected, n), nil)
	}
	return int32(n), nil
}
