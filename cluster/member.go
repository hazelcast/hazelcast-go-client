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

	"github.com/hazelcast/hazelcast-go-client/types"
)

// MemberInfo represents a member in the cluster.
type MemberInfo struct {
	Attributes map[string]string
	AddressMap map[EndpointQualifier]Address
	Address    Address
	UUID       types.UUID
	Version    MemberVersion
	LiteMember bool
}

func (mi MemberInfo) String() string {
	return fmt.Sprintf("%s:%s", mi.Address, mi.UUID)
}

// PublicAddress returns the public address and ok == true if member contains a public address.
func (mi MemberInfo) PublicAddress() (addr Address, ok bool) {
	for q, a := range mi.AddressMap {
		if q.Type == EndpointQualifierTypeClient && q.Identifier == "public" {
			return a, true
		}
	}
	return "", false
}

// MemberVersion is the version of the member
type MemberVersion struct {
	Major byte
	Minor byte
	Patch byte
}

type EndpointQualifier struct {
	Identifier string
	Type       EndpointQualifierType
}

type EndpointQualifierType int32

const (
	EndpointQualifierTypeMember   EndpointQualifierType = 0
	EndpointQualifierTypeClient   EndpointQualifierType = 1
	EndpointQualifierTypeWan      EndpointQualifierType = 2
	EndpointQualifierTypeRest     EndpointQualifierType = 3
	EndpointQualifierTypeMemCache EndpointQualifierType = 4
)

func (t EndpointQualifierType) String() string {
	switch t {
	case EndpointQualifierTypeMember:
		return "member"
	case EndpointQualifierTypeClient:
		return "client"
	case EndpointQualifierTypeWan:
		return "wan"
	case EndpointQualifierTypeRest:
		return "rest"
	case EndpointQualifierTypeMemCache:
		return "memcache"
	default:
		return "UNKNOWN"
	}
}
