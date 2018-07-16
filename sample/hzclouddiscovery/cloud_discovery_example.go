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
	"log"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
)

func main() {

	cfg := hazelcast.NewConfig()
	cfg.GroupConfig().SetName("exampleName")
	discoveryCfg := config.NewClientCloud()
	discoveryCfg.SetEnabled(true)
	discoveryCfg.SetDiscoveryToken("exampleToken")
	cfg.NetworkConfig().SetCloudConfig(discoveryCfg)

	client, _ := hazelcast.NewClientWithConfig(cfg)

	mp, _ := client.GetMap("exampleMap")
	mp.Put("exampleKey", "examplePut")

	res, _ := mp.Get("exampleKey")

	log.Println("Get returned : ", res)

	client.Shutdown()

}
