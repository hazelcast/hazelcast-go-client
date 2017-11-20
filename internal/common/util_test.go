// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"testing"
	"time"
)

func TestIsValidIpAddress(t *testing.T) {
	ip := "142.1.2.4"
	if ok := IsValidIpAddress(ip); !ok {
		t.Fatal("IsValidIpAddress failed")
	}
	ip = "123.2.1"
	if ok := IsValidIpAddress(ip); ok {
		t.Fatal("IsValidIpAddress failed")
	}
}
func TestGetIpAndPort(t *testing.T) {
	testAddress := "121.1.23.3:1231"
	if ip, port := GetIpAndPort(testAddress); ip != "121.1.23.3" || port != 1231 {
		t.Fatal("GetIPAndPort failed.")
	}
}
func TestGetTimeInMilliSeconds(t *testing.T) {
	var expected int64 = 100
	var baseTime int64 = 100
	if result := GetTimeInMilliSeconds(baseTime, time.Millisecond); result != expected {
		t.Fatal("An error in GetTimeInMilleSeconds()")
	}
	expected = expected * 1000
	if result := GetTimeInMilliSeconds(baseTime, time.Second); result != expected {
		t.Fatal("An error in GetTimeInMilleSeconds()")
	}
	expected = expected * 60
	if result := GetTimeInMilliSeconds(baseTime, time.Minute); result != expected {
		t.Fatal("An error in GetTimeInMilleSeconds()")
	}
	expected = expected * 60
	if result := GetTimeInMilliSeconds(baseTime, time.Hour); result != expected {
		t.Fatal("An error in GetTimeInMilleSeconds()")
	}
}
