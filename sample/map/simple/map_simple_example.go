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

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {

	config := hazelcast.NewConfig()
	config.NetworkConfig().AddAddress("127.0.0.1:5701")

	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	mp, _ := client.GetMap("simpleExample")

	mp.Put("key1", "value1")
	mp.Put("key2", "value2")

	result1, _ := mp.Get("key1")
	fmt.Println("key1 has the value of ", result1)

	result2, _ := mp.Get("key2")
	fmt.Println("key2 has the value of ", result2)

	mp.Clear()
	client.Shutdown()
}
