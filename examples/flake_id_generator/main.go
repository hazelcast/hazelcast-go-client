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
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	// number of pre-fetched IDs from cluster.
	idPrefetchCount int32 = 10
	// validity duration for pre-fetched IDs.
	idPrefetchExpiry = types.Duration(time.Minute)
)

func main() {
	ctx := context.Background()
	config := hazelcast.NewConfig()
	if err := config.AddFlakeIDGenerator("id-gen", idPrefetchCount, idPrefetchExpiry); err != nil {
		log.Fatal(err)
	}
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	idGen, err := client.GetFlakeIDGenerator(ctx, "id-gen")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		id, err := idGen.NewID(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("flakeID(%d):\t%d\n", i, id)
	}
	if err := client.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
