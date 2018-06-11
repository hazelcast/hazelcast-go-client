// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package iputil

import (
	"testing"
)

func TestIsValidIPAddress(t *testing.T) {
	ip := "142.1.2.4"
	if ok := IsValidIPAddress(ip); !ok {
		t.Fatal("IsValidIPAddress failed")
	}
	ip = "123.2.1"
	if ok := IsValidIPAddress(ip); ok {
		t.Fatal("IsValidIPAddress failed")
	}
}

func TestGetIpAndPort(t *testing.T) {
	testAddress := "121.1.23.3:1231"
	if ip, port := GetIPAndPort(testAddress); ip != "121.1.23.3" || port != 1231 {
		t.Fatal("GetIPAndPort failed.")
	}
}

func TestGetIpWithoutPort(t *testing.T) {
	testAddress := "121.1.23.3"
	if ip, port := GetIPAndPort(testAddress); ip != "121.1.23.3" || port != -1 {
		t.Fatal("GetIPAndPort failed.")
	}
}
