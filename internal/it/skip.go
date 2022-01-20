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
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal"
)

const (
	skipHzVersion     = "hz"
	skipClientVersion = "ver"
	skipOS            = "os"
	skipEnterprise    = "enterprise"
	skipNotEnterprise = "!enterprise"
	skipOSS           = "oss"
	skipNotOSS        = "!oss"
	enterpriseKey     = "HAZELCAST_ENTERPRISE_KEY"
)

var skipChecker = defaultSkipChecker()

/*
SkipIf can be used to skip a test case based on comma-separated conditions.
There are two kinds of conditions, comparisons and booleans.

Comparison conditions

Comparison conditions are in the following format:

	KEY OP [VERSION|STRING]

KEY is one of the following keys:

	hz: Hazelcast version
	ver: Go Client version
	os: Operating system name, taken from runtime.GOOS

hz and ver keys support the following operators:

	<, <=, =, !=, >=, >

os key supports the following operators:

	=, !=

VERSION has the following format:

	Major[.Minor[.Patch[...]]][-SUFFIX]

If minor, patch, etc. are not given, they are assumed to be 0.
A version with a suffix is less than a version without suffix, if their Major, Minor, Patch, ... are the same.

Boolean conditions

Boolean conditions are in the following format:

	[!]KEY

KEY is one of the following keys:

	enterprise: Whether the Hazelcast cluster is enterprise
				(existence of HAZELCAST_ENTERPRISE_KEY environment variable)
	oss: Whether the Hazelcast cluster is open source
		 (non-existence of HAZELCAST_ENTERPRISE_KEY environment variable)

! operator negates the value of the key.

Many Conditions

More than one condition may be specified by separating them with commas.
All conditions should be satisfied to skip.

	SkipIf(t, "ver > 1.1, hz = 5, os != windows, !enterprise")

You can use multiple SkipIf statements to skip if one one of the conditions is satisfied:

	// skip if the OS is windows or client version is greater than 1.3.2 and the Hazelcast cluster is open source:
	SkipIf(t, "os = windows")
	SkipIf(t, "ver > 1.3.2, oss")

*/
func SkipIf(t *testing.T, conditions string) {
	if skipChecker.CanSkip(conditions) {
		t.Skipf("Skipping test since: %s", conditions)
	}
}

type SkipChecker struct {
	hzVer      string
	ver        string
	os         string
	enterprise bool
}

// defaultSkipChecker creates and returns the default skip checker.
func defaultSkipChecker() SkipChecker {
	_, enterprise := os.LookupEnv(enterpriseKey)
	return SkipChecker{
		hzVer:      HzVersion(),
		ver:        internal.ClientVersion,
		os:         runtime.GOOS,
		enterprise: enterprise,
	}
}

// CheckHzVer evaluates left OP right and returns the result.
// left is the actual Hazelcast server version.
// op is the comparison operator.
// right is the given Hazelcast server version.
// Hazelcast server version is retrieved from HZ_VERSION environment variable.
func (s SkipChecker) CheckHzVer(op, right string) bool {
	return checkVersion(s.hzVer, op, right)
}

// CheckVer evaluates left OP right and returns the result.
// left is the actual client version.
// op is the comparison operator.
// right is the given client version.
func (s SkipChecker) CheckVer(op, right string) bool {
	return checkVersion(s.ver, op, right)
}

// CheckOS evaluates left OP right and returns the result.
// left is the actual operating system name.
// op is the comparison operator.
// right is the given operating system name.
// Consult runtime.GOOS for the valid operating system names.
func (s SkipChecker) CheckOS(op, right string) bool {
	return checkOS(s.os, op, right)
}

// Enterprise returns true if the actual Hazelcast server is Enterprise.
// The default skip checker considers non-blank HAZELCAST_ENTERPRISE_KEY as Hazelcast Enterprise.
func (s SkipChecker) Enterprise() bool {
	return s.enterprise
}

// OSS returns true if the actual Hazelcast server is open source.
// The default skip checker considers blank HAZELCAST_ENTERPRISE_KEY as Hazelcast open source.
func (s SkipChecker) OSS() bool {
	return !s.enterprise
}

// CanSkip skips returns true if all the given conditions evaluate to true.
// Separate conditions with commas (,).
func (s SkipChecker) CanSkip(condStr string) bool {
	conds := strings.Split(condStr, ",")
	for _, c := range conds {
		if !s.checkCondition(strings.TrimSpace(c)) {
			return false
		}
	}
	return true
}

func (s SkipChecker) checkCondition(cond string) bool {
	parts := strings.Split(cond, " ")
	left := parts[0]
	switch left {
	case skipHzVersion:
		ensureLen(parts, 3, cond, "hz = 5.0")
		return s.CheckHzVer(parts[1], parts[2])
	case skipClientVersion:
		ensureLen(parts, 3, cond, "ver = 5.0")
		return s.CheckVer(parts[1], parts[2])
	case skipOS:
		ensureLen(parts, 3, cond, "os = linux")
		return s.CheckOS(parts[1], parts[2])
	case skipEnterprise:
		ensureLen(parts, 1, cond, "enterprise")
		return s.Enterprise()
	case skipNotEnterprise:
		ensureLen(parts, 1, cond, "!enterprise")
		return !s.Enterprise()
	case skipOSS:
		ensureLen(parts, 1, cond, "oss")
		return s.OSS()
	case skipNotOSS:
		ensureLen(parts, 1, cond, "!oss")
		return !s.OSS()
	default:
		panic(fmt.Errorf(`unexpected test skip constant "%s" in %s`, parts[0], cond))
	}
}

func ensureLen(parts []string, expected int, condition, example string) {
	if len(parts) != expected {
		panic(fmt.Errorf(`unexpected format for %s, example of expected condition: "%s"`, condition, example))
	}
}

func checkVersion(left, operator, right string) bool {
	cmp := compareVersions(left, right)
	switch operator {
	case "=":
		return cmp == 0
	case "!=":
		return cmp != 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	default:
		panic(fmt.Errorf(`unexpected test skip operator "%s" to compare versions`, operator))
	}
}

func compareVersions(left, right string) int {
	var leftHasSuffix, rightHasSuffix bool
	leftHasSuffix, left = stripSuffix(left)
	rightHasSuffix, right = stripSuffix(right)
	// versionNumbers describe the numbers of the present version
	leftNums := strings.Split(left, ".")
	// checkNumbers describe the numbers received to test for
	rightNums := strings.Split(right, ".")
	// make rightNums and leftNums the same length by filling the shorter one with zeros.
	if len(rightNums) < len(leftNums) {
		rightNums = equalizeVersions(rightNums, leftNums)
	} else if len(leftNums) < len(rightNums) {
		leftNums = equalizeVersions(leftNums, rightNums)
	}
	for i := 0; i < len(rightNums); i++ {
		r := mustAtoi(rightNums[i])
		l := mustAtoi(leftNums[i])
		if r < l {
			return 1
		}
		if r > l {
			return -1
		}
	}
	// if the version number has a suffix, then it is prior to the non-suffixed one.
	// See: https://semver.org/#spec-item-9
	if leftHasSuffix {
		if rightHasSuffix {
			return 0
		}
		return -1
	}
	if rightHasSuffix {
		return 1
	}
	return 0
}

// stripSuffix checks and removes suffix, e.g., "-beta1" from the version.
func stripSuffix(version string) (hasSuffix bool, newVersion string) {
	newVersion = version
	hasSuffix = strings.Contains(version, "-")
	if hasSuffix {
		newVersion = strings.SplitN(newVersion, "-", 2)[0]
	}
	return
}

func mustAtoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Errorf("could not parse int %s: %w", s, err))
	}
	return n
}

func checkOS(left, operator, right string) bool {
	switch operator {
	case "=":
		return left == right
	case "!=":
		return left != right
	default:
		panic(fmt.Errorf(`unexpected test skip operator "%s" in "%s" condition`, operator, skipOS))
	}
}

// equalizeVersions makes minV and maxV the same length by filling minV with zeros and returns the new minV
func equalizeVersions(minV, maxV []string) []string {
	var i int
	newMinV := make([]string, len(maxV))
	for i = 0; i < len(minV); i++ {
		newMinV[i] = minV[i]
	}
	for ; i < len(maxV); i++ {
		newMinV[i] = "0"
	}
	return newMinV
}
