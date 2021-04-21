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

package versionutil

import "testing"

func TestCalculateVersion(t *testing.T) {
	testVersionString(t, "", -1)
	testVersionString(t, "a.3.7.5", -1)
	testVersionString(t, "3.a.5", -1)
	testVersionString(t, "3,7.5", -1)
	testVersionString(t, "3.7,5", -1)
	testVersionString(t, "10.99.RC1", -1)

	testVersionString(t, "3.7.2", 30702)
	testVersionString(t, "1.99.30", 19930)
	testVersionString(t, "3.7-SNAPSHOT", 30700)
	testVersionString(t, "3.7.2-SNAPSHOT", 30702)
	testVersionString(t, "10.99.2-SNAPSHOT", 109902)
	testVersionString(t, "10.99.30-SNAPSHOT", 109930)
	testVersionString(t, "10.99-RC1", 109900)

}

func testVersionString(t *testing.T, version string, expected int32) {
	if result := CalculateVersion(version); result != expected {
		t.Errorf("expected %d got %d", result, expected)
	}
}
