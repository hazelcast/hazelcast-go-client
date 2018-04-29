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
	// AddListener returns uuid which will be used to remove the listener.
	AddListener(listener interface{}) *string

	// RemoveListener removes the listener with the given registrationID.
	// RemoveListener returns true if successfully removed, false otherwise.
	RemoveListener(registrationID *string) bool

	// Members returns a slice of members.
	Members() []IMember

	// Member gets the member with the given address.
	Member(address IAddress) IMember

	// MemberByUUID gets the member with the given uuid.
	MemberByUUID(uuid string) IMember
}
