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
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"runtime"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/predicate"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	subroutineCount = 32
	entryCount      = 10000
	factoryID       = 66
	testValue       = "test"
)

func main() {
	startMapSoak()
}

func startMapSoak() {
	routineNumBefore := runtime.NumGoroutine()
	numbPtr := flag.Float64("hour", 0, "a float")
	addresses := flag.String("addresses", "", "addresses")
	flag.Parse()
	config := hazelcast.NewHazelcastConfig()
	processor := newSimpleEntryProcessor()
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryID, processor.identifiedFactory)
	addressSlice := strings.Split(*addresses, "-")
	for _, address := range addressSlice {
		config.ClientNetworkConfig().AddAddress(address)
	}
	client, err := hazelcast.NewHazelcastClientWithConfig(config)
	if err != nil {
		log.Println(err)
		return
	}

	mp, err := client.GetMap("testMap")

	if err != nil {
		log.Println(err)
		return
	}
	mp.AddEntryListener(simpleListener{}, false)
	wg := sync.WaitGroup{}
	log.Println("Soak test operations are starting!")
	for i := 0; i < subroutineCount; i++ {
		wg.Add(1)
		go func() {
			startTime := time.Now()
			for time.Since(startTime).Hours() < *numbPtr {
				key := strconv.Itoa(rand.Intn(entryCount))
				value := strconv.Itoa(rand.Intn(entryCount))
				op := rand.Intn(100)
				if op < 30 {
					_, err := mp.Get(key)
					if err != nil {
						log.Println("Error in Get() ", err)
					}
				} else if op < 60 {
					_, err := mp.Put(key, value)
					if err != nil {
						log.Println("Error in Put() ", err)
					}
				} else if op < 80 {
					_, err := mp.ValuesWithPredicate(predicate.Between("this", int32(0), int32(10)))
					if err != nil {
						log.Println("Error in ValuesWithPredicate() ", err)
					}
				} else {
					_, err := mp.ExecuteOnKey(key, processor)
					if err != nil {
						log.Println("Error in ExecuteOnKey() ", err)
					}
				}
			}
			wg.Done()
		}()

	}
	wg.Wait()
	client.Shutdown()
	log.Println("Soak test has finished!")
	time.Sleep(10 * time.Second)
	routineNumAfter := runtime.NumGoroutine()
	log.Printf("Number of go subroutines before: %d after: %d", routineNumBefore, routineNumAfter)

}

type simpleListener struct {
}

func (l *simpleListener) EntryAdded(event core.EntryEvent) {
	event.Key()
	event.Value()
	event.OldValue()
}

func (l *simpleListener) EntryUpdated(event core.EntryEvent) {
	event.Key()
	event.Value()
	event.OldValue()
}

func (l *simpleListener) EntryRemoved(event core.EntryEvent) {
	event.Key()
	event.Value()
	event.OldValue()
}

type simpleEntryProcessor struct {
	classID           int32
	value             string
	identifiedFactory *identifiedFactory
}

func newSimpleEntryProcessor() *simpleEntryProcessor {
	processor := &simpleEntryProcessor{classID: 1, value: testValue}
	identifiedFactory := &identifiedFactory{factoryID: factoryID, simpleEntryProcessor: processor}
	processor.identifiedFactory = identifiedFactory
	return processor
}

type identifiedFactory struct {
	simpleEntryProcessor *simpleEntryProcessor
	factoryID            int32
}

func (idf *identifiedFactory) Create(id int32) serialization.IdentifiedDataSerializable {
	if id == idf.simpleEntryProcessor.classID {
		return &simpleEntryProcessor{classID: 1}
	}
	return nil
}

func (p *simpleEntryProcessor) ReadData(input serialization.DataInput) error {
	var err error
	p.value, err = input.ReadUTF()
	return err
}

func (p *simpleEntryProcessor) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(p.value)
	return nil
}

func (p *simpleEntryProcessor) FactoryID() int32 {
	return p.identifiedFactory.factoryID
}

func (p *simpleEntryProcessor) ClassID() int32 {
	return p.classID
}
