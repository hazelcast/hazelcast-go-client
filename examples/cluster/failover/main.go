/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/types"
)

/*
In order to test this example:

1. Create two clusters running at localhost with names dev1 and dev2.
2. Run the code sample in this file
3. The client should connect to the `dev1` cluster and print out values.
4. Terminate `dev1` cluster, the client should connect to the dev2 cluster and continue printing out values.

*/

var loggingLevel = logger.DebugLevel

func makeKeyValue(i int) (key string, value string) {
	key = fmt.Sprintf("key-%d", i)
	value = fmt.Sprintf("value-%d", i)
	return
}

func updateFailover(fo *cluster.FailoverConfig) {
	cluster1 := cluster.Config{Name: "dev1"}
	cluster1.Network.SetAddresses("localhost:5701")
	cluster1.ConnectionStrategy.Timeout = types.Duration(5 * time.Second)
	cluster2 := cluster.Config{Name: "dev2"}
	cluster2.Network.SetAddresses("localhost:5702")
	cluster2.ConnectionStrategy.Timeout = types.Duration(15 * time.Second)
	fo.Enabled = true
	fo.TryCount = 100
	fo.SetConfigs(cluster1, cluster2)
}

func getClient(ctx context.Context) *hazelcast.Client {
	config := hazelcast.Config{}
	config.Logger.Level = loggingLevel
	updateFailover(&config.Failover)
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func main() {
	ctx := context.TODO()
	client := getClient(ctx)
	m, err := client.GetMap(ctx, "sample-map")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		key, value := makeKeyValue(i)
		log.Printf("writing %s=%s", key, value)
		if err = m.Set(ctx, key, value); err != nil {
			log.Fatal(err)
		}
		log.Printf("reading %s", key)
		readValue, err := m.Get(ctx, key)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("read: %v\n", readValue)
		time.Sleep(1000 * time.Millisecond)
	}
	if err = client.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
