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

package cluster

import (
	"fmt"
	"net"
	"strconv"
)

type Address string

func NewAddress(host string, port int32) Address {
	return Address(fmt.Sprintf("%s:%d", host, port))
}

func (a Address) Host() string {
	if host, _, err := net.SplitHostPort(string(a)); err != nil {
		return ""
	} else {
		return host
	}
}

func (a Address) Port() int {
	if _, portStr, err := net.SplitHostPort(string(a)); err != nil {
		return 0
	} else if port, err := strconv.Atoi(portStr); err != nil {
		return 0
	} else {
		return port
	}
}

func (a Address) String() string {
	return string(a)
}
