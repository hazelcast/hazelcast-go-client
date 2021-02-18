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
	reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
	var wg sync.WaitGroup
	wg.Add(10)
	reliableTopic.AddMessageListener(&reliableTopicMessageListener{wg: &wg})

	go func() {
		for i := 0; i < 10; i++ {
			reliableTopic.Publish("Message " + strconv.Itoa(i))
		}
	}()
	wg.Wait()
	reliableTopic.Destroy()
	client.Shutdown()
}

type reliableTopicMessageListener struct {
	wg *sync.WaitGroup
}

func (r *reliableTopicMessageListener) OnMessage(message core.Message) error {
	fmt.Println("Got message: ", message.MessageObject())
	fmt.Println("Publishing Time: ", message.PublishTime())
	r.wg.Done()
	return nil
}

func (r *reliableTopicMessageListener) RetrieveInitialSequence() int64 {
	return -1
}

func (r *reliableTopicMessageListener) StoreSequence(sequence int64) {
}

func (r *reliableTopicMessageListener) IsLossTolerant() bool {
	return true
}

func (r *reliableTopicMessageListener) IsTerminal(err error) (bool, error) {
	return true, nil
}
