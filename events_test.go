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
package hazelcast_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
)

func TestLifecycleState_String(t *testing.T) {
	testCases := []struct {
		state          hazelcast.LifecycleState
		expectedString string
	}{
		{state: hazelcast.LifecycleStateStarting, expectedString: "starting"},
		{state: hazelcast.LifecycleStateStarted, expectedString: "started"},
		{state: hazelcast.LifecycleStateShuttingDown, expectedString: "shutting down"},
		{state: hazelcast.LifecycleStateShutDown, expectedString: "shutdown"},
		{state: hazelcast.LifecycleStateConnected, expectedString: "client connected"},
		{state: hazelcast.LifecycleStateDisconnected, expectedString: "client disconnected"},
		{state: hazelcast.LifecycleStateChangedCluster, expectedString: "changed cluster"},
		{state: hazelcast.LifecycleStateChangedCluster + 1, expectedString: "UNKNOWN"},
	}
	for _, tc := range testCases {
		t.Run(tc.expectedString, func(t *testing.T) {
			got := tc.state.String()
			if tc.state.String() != tc.expectedString {
				t.Fatalf("got %v want %v", got, tc.expectedString)
			}
		})
	}
}

func Test_EventName(t *testing.T) {
	testCases := []struct {
		event          interface{}
		expectedString string
	}{
		{event: &hazelcast.LifecycleStateChanged{}, expectedString: "lifecyclestatechanged"},
		{event: &hazelcast.MessagePublished{}, expectedString: "messagepublished"},
		{event: &hazelcast.EntryNotified{}, expectedString: "entrynotified"},
		{event: &hazelcast.QueueItemNotified{}, expectedString: "queue.itemnotified"},
		{event: &hazelcast.ListItemNotified{}, expectedString: "list.itemnotified"},
		{event: &hazelcast.SetItemNotified{}, expectedString: "set.itemnotified"},
		{event: &hazelcast.DistributedObjectNotified{}, expectedString: "distributedobjectnotified"},
	}
	for _, tc := range testCases {
		t.Run(tc.expectedString, func(t *testing.T) {
			e, _ := tc.event.(event.Event)
			assert.Equal(t, tc.expectedString, e.EventName())
		})
	}
}
