// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package main

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/hazelcast/hazelcast-go-client/v3"
	"github.com/hazelcast/hazelcast-go-client/v3/core"
)

func main() {

	config := hazelcast.NewConfig()
	config.NetworkConfig().AddAddress("127.0.0.1:5701")

	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	topic, _ := client.GetTopic("myTopic")

	var wg sync.WaitGroup
	wg.Add(10)
	topicMessageListener := &topicMessageListener{wg: &wg}
	topic.AddMessageListener(topicMessageListener)

	go func() {
		for i := 0; i < 10; i++ {
			topic.Publish("Message " + strconv.Itoa(i))
		}
	}()

	wg.Wait()

	topic.Destroy()
	client.Shutdown()
}

type topicMessageListener struct {
	wg *sync.WaitGroup
}

func (l *topicMessageListener) OnMessage(message core.Message) error {
	fmt.Println("Got message: ", message.MessageObject())
	fmt.Println("Publishing Time: ", message.PublishTime())
	l.wg.Done()
	return nil
}
