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

package main

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {

	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().AddAddress("127.0.0.1:5701")

	client, err := hazelcast.NewHazelcastClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	set, _ := client.Set("students")

	set.Add("Furkan")
	set.Add("Talha")

	items, _ := set.ToSlice()
	fmt.Println("ToSlice: ", items)

	found, _ := set.Contains("Furkan")
	fmt.Println("Contains: ", found)

	size, _ := set.Size()
	fmt.Println("Size: ", size)

	set.Destroy()
	client.Shutdown()
}
