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
)

func TestEntryNotified_EventName(t *testing.T) {
	en := hazelcast.EntryNotified{}
	assert.Equal(t, en.EventName(), "entrynotified")
}

func TestLifecycleState_String(t *testing.T) {
	for _, tc := range []struct {
		state          hazelcast.LifecycleState
		expectedString string
	}{
		{hazelcast.LifecycleStateStarting, "starting"},
		{hazelcast.LifecycleStateStarted, "started"},
		{hazelcast.LifecycleStateShuttingDown, "shutting down"},
		{hazelcast.LifecycleStateShutDown, "shutdown"},
		{hazelcast.LifecycleStateConnected, "client connected"},
		{hazelcast.LifecycleStateDisconnected, "client disconnected"},
		{hazelcast.LifecycleStateChangedCluster, "changed cluster"},
		{hazelcast.LifecycleStateChangedCluster + 1, "UNKNOWN"},
	} {
		t.Run(tc.expectedString, func(t *testing.T) {
			got := tc.state.String()
			if tc.state.String() != tc.expectedString {
				t.Fatalf("got %v want %v", got, tc.expectedString)
			}
		})
	}
}

func TestLifecycleStateChanged_EventName(t *testing.T) {
	// event name is independent of the state
	event := hazelcast.LifecycleStateChanged{}
	assert.Equal(t, event.EventName(), "lifecyclestatechanged")
}

func TestMessagePublished_EventName(t *testing.T) {
	// event name is independent of the state
	event := hazelcast.MessagePublished{}
	assert.Equal(t, event.EventName(), "messagepublished")
}

func TestQueueItemNotified_EventName(t *testing.T) {
	event := hazelcast.QueueItemNotified{}
	assert.Equal(t, event.EventName(), "queue.itemnotified")
}

func TestListItemNotified_EventName(t *testing.T) {
	event := hazelcast.ListItemNotified{}
	assert.Equal(t, event.EventName(), "list.itemnotified")
}

func TestSetItemNotified_EventName(t *testing.T) {
	event := hazelcast.SetItemNotified{}
	assert.Equal(t, event.EventName(), "set.itemnotified")
}

func TestDistributedObjectNotified_EventName(t *testing.T) {
	event := hazelcast.DistributedObjectNotified{}
	assert.Equal(t, event.EventName(), "distributedobjectnotified")
}
