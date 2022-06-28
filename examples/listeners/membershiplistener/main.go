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
	"sync"

	"github.com/hazelcast/hazelcast-go-client"
	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
)

func main() {
	var (
		wgAdded,
		wgRemoved sync.WaitGroup
	)
	wgAdded.Add(1)
	wgRemoved.Add(1)
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func(client *hazelcast.Client, ctx context.Context) {
		err = client.Shutdown(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}(client, ctx)
	_, err = client.AddMembershipListener(func(event pubcluster.MembershipStateChanged) {
		switch event.State {
		case pubcluster.MembershipStateAdded:
			wgAdded.Done()
		case pubcluster.MembershipStateRemoved:
			wgRemoved.Done()
		}
		log.Printf("Event: %s, member-id: %s\n", event.State, event.Member.UUID)
	})
	if err != nil {
		log.Fatal(err)
	}
	wgRemoved.Wait()
	wgAdded.Wait()
}
