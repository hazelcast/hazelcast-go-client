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

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
)

func main() {

	config := hazelcast.NewConfig()
	config.NetworkConfig().AddAddress("127.0.0.1:5701")

	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	ringbuffer, _ := client.GetRingbuffer("myRingbuffer")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			ringbuffer.Add("item "+strconv.Itoa(i), core.OverflowPolicyOverwrite)
		}
	}()

	go func() {
		defer wg.Done()
		sequence, _ := ringbuffer.HeadSequence()
		for sequence < 100 {
			item, _ := ringbuffer.ReadOne(sequence)
			sequence++
			fmt.Println("Reading value " + item.(string))

		}
	}()

	wg.Wait()

	ringbuffer.Destroy()
	client.Shutdown()
}
