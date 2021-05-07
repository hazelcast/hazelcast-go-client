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

package event

import (
	"reflect"
	"strconv"
)

// MakeSubscriptionID creates subscription ID from a function
func MakeSubscriptionID(fun interface{}) int64 {
	value := reflect.ValueOf(fun)
	if value.Kind() != reflect.Func {
		panic("not a func")
	}
	return int64(value.Pointer())
}

func ParseSubscriptionID(id string) (int64, error) {
	return strconv.ParseInt(id, 10, 64)
}

func FormatSubscriptionID(id int64) string {
	return strconv.FormatInt(id, 10)
}
