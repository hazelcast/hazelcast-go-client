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

package skip_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/skip"
)

type skipTestCase struct {
	name       string
	conditions string
	panics     bool
	skips      bool
}

func TestSkipIf(t *testing.T) {
	skipChecker := skip.Checker{
		HzVer:      "5.1-SNAPSHOT",
		Ver:        "1.2.1",
		OS:         "windows",
		Arch:       "386",
		Enterprise: true,
		Race:       true,
		SSL:        true,
		Slow:       true,
		Flaky:      true,
		All:        true,
	}
	testCases := []skipTestCase{
		// Check parsing
		panics("version < 1.0.0"),
		panics("os windows"),
		panics("enterprise = os"),
		panics("enterprise = os, , Ver > 0"),
		panics("enterprise = os,, Ver > 0"),
		// Check OS based on non-existing OS
		skips("os != non-existent"),
		noSkip("os = non-existent"),
		// Check client version
		skips("Ver > 0"),
		skips("Ver > 1"),
		skips("Ver > 1.1"),
		skips("Ver > 1.1.0"),
		skips("Ver > 1.1.1"),
		skips("Ver > 1.2.0"),
		skips("Ver > 1.2"),
		skips("Ver >= 1.2.0"),
		skips("Ver >= 1.2.1"),
		skips("Ver >= 1.2"),
		noSkip("Ver = 1.2.0"),
		skips("Ver = 1.2.1"),
		noSkip("Ver = 1.2"),
		noSkip("Ver <= 1.2.0"),
		skips("Ver <= 1.2.1"),
		noSkip("Ver <= 1.2"),
		noSkip("Ver < 1.2.0"),
		noSkip("Ver < 1.2.1"),
		noSkip("Ver < 1.2"),
		skips("Ver < 1.2.2"),
		skips("Ver < 1.3"),
		skips("Ver < 2"),
		skips("Ver != 1.2.0"),
		noSkip("Ver != 1.2.1"),
		skips("Ver != 1.2"),
		skips("Ver != 1"),
		skips("Ver != 1.3"),
		skips("Ver != 2"),
		skips("Ver ~ 1"),
		skips("Ver ~ 1.2"),
		skips("Ver ~ 1.2.1"),
		noSkip("Ver ~ 2"),
		noSkip("Ver ~ 1.3"),
		noSkip("Ver ~ 1.2.2"),
		// Check Hazelcast version
		skips("hz > 0"),
		skips("hz > 1"),
		skips("hz > 4"),
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
		noSkip("hz = 5.1"),
		noSkip("hz = 5"),
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
		skips("hz ~ 5"),
		skips("hz ~ 5.1"),
		skips("hz ~ 5.1.0"),
		skips("hz ~ 5.1.0-SNAPSHOT"),
		skips("hz ~ 5.1.0-preview.2"),
		noSkip("hz ~ 6"),
		noSkip("hz ~ 5.2"),
		noSkip("hz ~ 5.1.2"),
		// check OS
		skips("os = windows"),
		noSkip("os = linux"),
		skips("os != darwin"),
		noSkip("os != windows"),
		// check Arch
		skips("arch = 386"),
		noSkip("arch = amd64"),
		skips("arch != amd64"),
		noSkip("arch != 386"),
		skips("arch ~ 32bit"),
		// check Enterprise
		skips("enterprise"),
		noSkip("!enterprise"),
		// check OSSs
		skips("!oss"),
		noSkip("oss"),
		// check race
		skips("race"),
		noSkip("!race"),
		// check SSL
		skips("ssl"),
		noSkip("!ssl"),
		// check slow
		skips("slow"),
		noSkip("!slow"),
		// check flaky
		skips("flaky"),
		noSkip("!flaky"),
		// check all
		skips("all"),
		noSkip("!all"),
		// Multiple conditions
		skips("hz > 5.0, hz < 5.1.0, Ver = 1.2.1, enterprise, !oss, os = windows, os != darwin, arch = 386, arch != amd64"),
		noSkip("hz > 5.0, Ver != 1.2.1"),
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sc := skipChecker
			if tc.panics {
				assert.Panics(t, func() {
					sc.CanSkip(tc.conditions)
				})
				return
			}
			assert.Equal(t, tc.skips, sc.CanSkip(tc.conditions))
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
