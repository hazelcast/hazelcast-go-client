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

package it

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type skipTestCase struct {
	name       string
	conditions string
	panics     bool
	skips      bool
}

func TestSkipIf(t *testing.T) {
	skipChecker := SkipChecker{
		hzVer:      "5.1-SNAPSHOT",
		ver:        "1.2.0",
		os:         "windows",
		enterprise: true,
	}
	testCases := []skipTestCase{
		// Check parsing
		panics("version < 1.0.0"),
		panics("os windows"),
		panics("enterprise = os"),
		panics("enterprise = os, , ver > 0"),
		panics("enterprise = os,, ver > 0"),
		// Check OS based on non-existing OS
		skips("os != non-existent"),
		noSkip("os = non-existent"),
		// Check client version
		skips("ver > 0"),
		skips("ver > 1"),
		skips("ver > 1.1"),
		skips("ver > 1.1.0"),
		skips("ver > 1.1.1"),
		noSkip("ver > 1.2.0"),
		noSkip("ver > 1.2"),
		skips("ver >= 1.2.0"),
		skips("ver >= 1.2"),
		skips("ver = 1.2.0"),
		skips("ver = 1.2"),
		skips("ver <= 1.2.0"),
		skips("ver <= 1.2"),
		noSkip("ver < 1.2.0"),
		noSkip("ver < 1.2"),
		skips("ver < 1.2.1"),
		skips("ver < 1.3"),
		skips("ver < 2"),
		noSkip("ver != 1.2.0"),
		noSkip("ver != 1.2"),
		skips("ver != 1"),
		skips("ver != 1.3"),
		skips("ver != 2"),
		// Check Hazelcast version
		skips("hz > 0"),
		skips("hz > 1"),
		skips("hz > 5"),
		skips("hz > 5.0"),
		skips("hz > 5.0.0"),
		skips("hz > 5.0.14562"),
		noSkip("hz > 5.1"),
		noSkip("hz > 5.1.0"),
		noSkip("hz > 5.1-preview.1"),
		skips("hz >= 5"),
		skips("hz >= 5.1-SNAPSHOT"),
		skips("hz >= 5.1.0-SNAPSHOT"),
		skips("hz >= 5.1.0-preview.1"),
		skips("hz = 5.1.0-SNAPSHOT"),
		skips("hz = 5.1.0-preview.1"),
		skips("hz = 5.1-SNAPSHOT"),
		skips("hz <= 5.1.0"),
		skips("hz <= 5.1"),
		skips("hz <= 5.1-SNAPSHOT"),
		skips("hz < 5.1.0"),
		skips("hz < 5.1"),
		skips("hz < 6"),
		skips("hz < 6.0.2"),
		noSkip("hz != 5.1.0-SNAPSHOT"),
		noSkip("hz != 5.1-preview.2"),
		skips("hz != 1"),
		skips("hz != 1.3"),
		skips("hz != 2"),
		// check OS
		skips("os = windows"),
		noSkip("os = linux"),
		skips("os != darwin"),
		noSkip("os != windows"),
		// enterprise
		skips("enterprise"),
		noSkip("!enterprise"),
		// oss
		skips("!oss"),
		noSkip("oss"),
		// Multiple conditions
		skips("hz > 5.0, hz < 5.1.0, ver = 1.2, enterprise, !oss, os = windows, os != darwin"),
		noSkip("hz > 5.0, ver != 1.2"),
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sc := skipChecker
			if tc.panics {
				assert.Panics(t, func() { sc.CanSkip(tc.conditions) })
				return
			}
			skip := sc.CanSkip(tc.conditions)
			assert.Equal(t, tc.skips, skip)
		})
	}
}

func skips(cond string) skipTestCase {
	return skipTestCase{
		name:       fmt.Sprintf("skips %s", cond),
		conditions: cond,
		skips:      true,
	}
}

func noSkip(cond string) skipTestCase {
	return skipTestCase{
		name:       fmt.Sprintf("doesn't skip %s", cond),
		conditions: cond,
		skips:      false,
	}
}

func panics(cond string) skipTestCase {
	return skipTestCase{
		name:       fmt.Sprintf("panics %s", cond),
		conditions: cond,
		panics:     true,
	}
}
