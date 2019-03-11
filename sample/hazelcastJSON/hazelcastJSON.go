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

package main

import (
	"encoding/json"

	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/predicate"
)

type person struct {
	Age  int
	Name string
}

func main() {
	clientConfig := hazelcast.NewConfig()

	clientConfig.NetworkConfig().AddAddress("127.0.0.1:5701")

	client, _ := hazelcast.NewClientWithConfig(clientConfig)
	mp, _ := client.GetMap("sampleMap")

	person1 := person{
		Age: 30, Name: "Name1",
	}

	person2 := person{
		Age: 40, Name: "Name2",
	}
	jsonStr1, _ := json.Marshal(person1)
	jsonStr2, _ := json.Marshal(person2)
	mp.Put("person1", core.HazelcastJSON{JSONString: jsonStr1})
	mp.Put("person2", core.HazelcastJSON{JSONString: jsonStr2})

	greaterEqual := predicate.GreaterThan("Age", int32(35))
	result, _ := mp.ValuesWithPredicate(greaterEqual)

	var resultPerson person
	json.Unmarshal(result[0].(core.HazelcastJSON).JSONString, &resultPerson)
	fmt.Println(resultPerson.Age)  // 40
	fmt.Println(resultPerson.Name) // Name2

	client.Shutdown()

}
