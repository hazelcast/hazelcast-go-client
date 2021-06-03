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

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

const clusterName = "PUT-YOUR-CLUSTER-NAME-HERE!"
const token = "PUT-YOUR-TOKEN-HERE!"

var loggingLevel = logger.WarnLevel

func makeKeyValue(i int) (key string, value string) {
	key = fmt.Sprintf("key-%d", i)
	value = fmt.Sprintf("value-%d", i)
	return
}

func getClient() *hazelcast.Client {
	config := hazelcast.NewConfig()
	config.LoggerConfig.Level = loggingLevel
	config.ClusterConfig.Name = clusterName
	cc := &config.ClusterConfig.HazelcastCloudConfig
	cc.Enabled = true
	cc.Token = token
	client, err := hazelcast.StartNewClientWithConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func main() {
	client := getClient()
	m, err := client.GetMap("sample-map")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key, value := makeKeyValue(i)
		log.Printf("writing %s=%s", key, value)
		if err = m.Set(key, value); err != nil {
			log.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i := 0; i < 100; i++ {
		key, value := makeKeyValue(i)
		log.Printf("reading %s", key)
		readValue, err := m.Get(key)
		if err != nil {
			log.Fatal(err)
		}
		if value != readValue {
			log.Printf("unexpected value: %s != %s", value, readValue)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err = client.Shutdown(); err != nil {
		log.Fatal(err)
	}
}
