/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package it

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/it/skip"
)

/*
SkipIf can be used to skip a test case based on comma-separated conditions.
Deprecated: Use skip.If instead.
*/
func SkipIf(t *testing.T, conditions string) {
	skip.If(t, conditions)
}

// MarkSlow marks a test "slow", so it is run only when slow test mode is enabled.
// Note that if "all" mode is enabled, the test runs disregard of whether slow test mode is enabled or disabled.
func MarkSlow(t *testing.T) {
	skip.If(t, "!slow, !all")
}

// MarkFlaky marks a test "flaky", so it is run only when flaky test mode is enabled.
// Note that if "all" mode is enabled, the test runs disregard of whether flaky test mode is enabled or disabled.
func MarkFlaky(t *testing.T, see ...string) {
	t.Logf("Note: %s is a known flaky test, it will run only when enabled.", t.Name())
	for _, s := range see {
		t.Logf("See: %s", s)
	}
	skip.If(t, "!flaky, !all")
}

// MarkRacy marks a test "racy", so it is run only when race detector is not enabled.
// Note that if "all" mode is enabled, the test runs disregard of whether the race detector is enabled or disabled.
func MarkRacy(t *testing.T, see ...string) {
	t.Logf("Note: %s is a test with a known race condition, it won't run when race detector is enabled.", t.Name())
	for _, s := range see {
		t.Logf("See: %s", s)
	}
	skip.If(t, "race, !all")
}
