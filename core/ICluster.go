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

package core

// ICluster is a cluster service for Hazelcast clients.
type ICluster interface {
	// AddListener registers the given listener.
	// AddListener returns UUID which will be used to remove the listener.
	AddListener(listener interface{}) *string

	// RemoveListener removes the listener with the given registrationId.
	// RemoveListener returns true if successfully removed, false otherwise.
	RemoveListener(registrationId *string) bool

	// GetMemberList returns a slice of members.
	GetMemberList() []IMember

	// GetMember gets the member with the given address.
	GetMember(address IAddress) IMember

	// GetMemberByUuid gets the member with the given uuid.
	GetMemberByUuid(uuid string) IMember
}
