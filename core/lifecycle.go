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

// ILifecycle is a lifecycle service for Hazelcast clients.
type Lifecycle interface {
	// AddListener adds a listener object to listen for lifecycle events.
	// AddListener returns the registrationId.
	AddListener(listener interface{}) string

	// RemoveListener removes lifecycle listener with the given registrationId.
	// RemoveListener returns true if the listener is removed successfully, false otherwise.
	RemoveListener(registrationId *string) bool
}
