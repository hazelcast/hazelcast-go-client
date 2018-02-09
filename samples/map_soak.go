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
	"flag"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

const subroutineCount = 32
const entryCount = 10000

func main() {
	startMapSoak()
}

func startMapSoak() {
	numbPtr := flag.Float64("hour", 48, "a float")
	addresses := flag.String("addresses", "", "addresses")
	flag.Parse()
	config := hazelcast.NewHazelcastConfig()
	processor := newSimpleEntryProcessor("test", 66)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryId, processor.identifiedFactory)
	addressSlice := strings.Split(*addresses, "-")
	for _, address := range addressSlice {
		config.ClientNetworkConfig().AddAddress(address)
	}
	client, err := hazelcast.NewHazelcastClientWithConfig(config)
	if err != nil {
		log.Println(err)
	}

	mp, err := client.GetMap("testMap")

	if err != nil {
		log.Println(err)
	}
	mp.AddEntryListener(simpleListener{}, false)
	wg := sync.WaitGroup{}
	log.Println("Soak test operations are starting!")
	for i := 0; i < subroutineCount; i++ {
		wg.Add(1)
		go func() {
			startTime := time.Now()
			for time.Duration(time.Since(startTime)).Hours() < *numbPtr {
				key := strconv.Itoa(rand.Intn(entryCount))
				value := strconv.Itoa(rand.Intn(entryCount))
				op := rand.Intn(100)
				if op < 30 {
					_, err := mp.Get(key)
					if err != nil {
						log.Println("error in Get() ", err)
					}
				} else if op < 60 {
					_, err := mp.Put(key, value)
					if err != nil {
						log.Println("error in Put() ", err)
					}
				} else if op < 80 {
					_, err := mp.ValuesWithPredicate(core.Between("this", int32(0), int32(10)))
					if err != nil {
						log.Println("error in ValuesWithPredicate() ", err)
					}
				} else {
					_, err := mp.ExecuteOnKey(key, processor)
					if err != nil {
						log.Println("error in ExecuteOnKey() ", err)
					}
				}
			}
			wg.Done()
		}()

	}
	wg.Wait()
	log.Println("Soak test has finished!")
}

type simpleListener struct {
	event    core.IEntryEvent
	mapEvent core.IMapEvent
}

func (addEntry *simpleListener) EntryAdded(event core.IEntryEvent) {
	event.Key()
	event.Value()
	event.OldValue()
}
func (addEntry *simpleListener) EntryUpdated(event core.IEntryEvent) {
	event.Key()
	event.Value()
	event.OldValue()
}
func (addEntry *simpleListener) EntryRemoved(event core.IEntryEvent) {
	event.Key()
	event.Value()
	event.OldValue()
}

type simpleEntryProcessor struct {
	classId           int32
	value             string
	identifiedFactory *identifiedFactory
}

func newSimpleEntryProcessor(value string, factoryId int32) *simpleEntryProcessor {
	processor := &simpleEntryProcessor{classId: 1, value: value}
	identifiedFactory := &identifiedFactory{factoryId: factoryId, simpleEntryProcessor: processor}
	processor.identifiedFactory = identifiedFactory
	return processor
}

type identifiedFactory struct {
	simpleEntryProcessor *simpleEntryProcessor
	factoryId            int32
}

func (identifiedFactory *identifiedFactory) Create(id int32) IdentifiedDataSerializable {
	if id == identifiedFactory.simpleEntryProcessor.classId {
		return &simpleEntryProcessor{classId: 1}
	} else {
		return nil
	}
}

func (simpleEntryProcessor *simpleEntryProcessor) ReadData(input DataInput) error {
	var err error
	simpleEntryProcessor.value, err = input.ReadUTF()
	return err
}

func (simpleEntryProcessor *simpleEntryProcessor) WriteData(output DataOutput) error {
	output.WriteUTF(simpleEntryProcessor.value)
	return nil
}

func (simpleEntryProcessor *simpleEntryProcessor) FactoryId() int32 {
	return simpleEntryProcessor.identifiedFactory.factoryId
}

func (simpleEntryProcessor *simpleEntryProcessor) ClassId() int32 {
	return simpleEntryProcessor.classId
}
