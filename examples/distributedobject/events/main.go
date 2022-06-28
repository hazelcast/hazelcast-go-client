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
	"log"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
)

func createDestroyMap(ctx context.Context, client *hazelcast.Client, name string) error {
	log.Println("Creating a map.")
	m, err := client.GetMap(ctx, "t1")
	if err != nil {
		return err
	}
	time.Sleep(1 * time.Second)
	log.Println("Destroying the map.")
	if err = m.Destroy(ctx); err != nil {
		return err
	}
	return nil
}

func main() {
	ctx := context.Background()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Adding the event listeners.")
	subID, err := client.AddDistributedObjectListener(ctx, func(e hazelcast.DistributedObjectNotified) {
		log.Println(e.EventType, e.ServiceName, e.ObjectName)
	})
	if err != nil {
		log.Fatal(err)
	}
	// creating a map an destroying it afterwards
	if err = createDestroyMap(ctx, client, "my-map-1"); err != nil {
		log.Fatal(err)
	}
	log.Println("Removing the event listeners.")
	if err := client.RemoveDistributedObjectListener(ctx, subID); err != nil {
		log.Fatal(err)
	}
	log.Println("The event listeners was removed, no DistributedObject events will be received from now on...")
	// creating a map an destroying it afterwards
	if err = createDestroyMap(ctx, client, "my-map-2"); err != nil {
		log.Fatal(err)
	}
}
