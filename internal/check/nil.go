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

package check

import "reflect"

/*
Nil does proper nil check for interface{} values taken from users.
See: https://golang.org/doc/faq#nil_error
*/
func Nil(arg interface{}) bool {
	if arg == nil {
		return true
	}
	// TODO: find ways to remove this check, we shouldn't rely on reflection to serialize every single value.
	value := reflect.ValueOf(arg)
	return value.Kind() == reflect.Ptr && value.IsNil()
}
