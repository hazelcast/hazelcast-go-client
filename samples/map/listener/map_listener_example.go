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
	"sync"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
)

type entryListener struct {
	wg *sync.WaitGroup
}

func (l *entryListener) EntryAdded(event core.IEntryEvent) {
	fmt.Println("Got an event, key: ", event.Key(), " value: ", event.Value())
	l.wg.Done()
}

func main() {

	client, err := hazelcast.NewHazelcastClient()
	if err != nil {
		fmt.Println(err)
		return
	}

	mp, _ := client.Map("testMap")

	var wg = new(sync.WaitGroup)
	entryListener := &entryListener{wg: wg}

	registrationID, err := mp.AddEntryListener(entryListener, true)
	if err != nil {
		fmt.Println(err)
		return
	}

	wg.Add(1)
	mp.Put("testKey", "testValue")
	wg.Wait() //Wait for the event

	mp.RemoveEntryListener(registrationID)
	client.Shutdown()

}
