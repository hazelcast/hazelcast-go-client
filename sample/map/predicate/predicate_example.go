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
	"strconv"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core/predicate"
)

func main() {
	config := hazelcast.NewConfig()
	config.NetworkConfig().AddAddress("127.0.0.1:5701")

	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
	}
	mp, _ := client.GetMap("predicateExample")

	for i := 0; i < 50; i++ {
		mp.Put("key"+strconv.Itoa(i), int32(i))
	}

	betweenPredicate := predicate.Between("this", int32(5), int32(35))

	set, err := mp.EntrySetWithPredicate(betweenPredicate)
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < len(set); i++ {
		fmt.Println(set[i].Key(), set[i].Value())
	}

	mp.Clear()
	client.Shutdown()
}
