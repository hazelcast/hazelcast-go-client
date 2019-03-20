// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package orgwebsite

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
)

type topicMessageListener struct {
}

func (*topicMessageListener) OnMessage(message core.Message) error {
	fmt.Println("Got message: ", message.MessageObject())
	return nil
}

func topicSampleRun() {
	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewClient()
	// Get a Topic called "my-distributed-topic"
	topic, _ := hz.GetTopic("my-distributed-topic")
	// Add a Listener to the Topic
	topic.AddMessageListener(&topicMessageListener{})
	// Publish a message to the Topic
	topic.Publish("Hello to distributed world")
	// Shutdown this hazelcast client
	hz.Shutdown()
}
