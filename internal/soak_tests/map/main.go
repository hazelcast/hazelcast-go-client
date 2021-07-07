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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hazelcast/hazelcast-go-client/serialization"

	"github.com/hazelcast/hazelcast-go-client/predicate"

	"github.com/hazelcast/hazelcast-go-client"
)

const displayDur = 10 * time.Second
const entryCount = 10_000
const goroutineCount = 10

var remainingGoroutines = int32(goroutineCount)

var tic time.Time

type Stats struct {
	Id      int
	opCount int64
}

func (s *Stats) IncOpCount() {
	atomic.AddInt64(&s.opCount, 1)
}

func (s *Stats) OpCountAndReset() int64 {
	return atomic.SwapInt64(&s.opCount, 0)
}

const simpleEntryProcessorFactoryID = 66
const simpleEntryProcessorClassID = 1

type SimpleEntryProcessor struct {
	value string
}

func (s SimpleEntryProcessor) FactoryID() int32 {
	return simpleEntryProcessorFactoryID
}

func (s SimpleEntryProcessor) ClassID() int32 {
	return simpleEntryProcessorClassID
}

func (s SimpleEntryProcessor) WriteData(output serialization.DataOutput) {
	output.WriteString(s.value)
}

func (s *SimpleEntryProcessor) ReadData(input serialization.DataInput) {
	s.value = input.ReadString()
}

type Factory struct {
}

func (f Factory) Create(id int32) serialization.IdentifiedDataSerializable {
	if id == simpleEntryProcessorClassID {
		return &SimpleEntryProcessor{}
	}
	panic(fmt.Sprintf("unknown class ID: %d", id))
}

func (f Factory) FactoryID() int32 {
	return simpleEntryProcessorFactoryID
}

func displayStats(sts []*Stats) {
	log.Println(strings.Repeat("*", 40))
	totalOp := int64(0)
	hanged := []int{}
	toc := time.Now()
	period := toc.Sub(tic)
	tic = toc
	for _, st := range sts {
		total := st.OpCountAndReset()
		totalOp += total
		if total == 0 {
			hanged = append(hanged, st.Id)
		}
	}
	if len(hanged) == 0 {
		log.Println("All goroutines worked without hanging")
	} else {
		log.Printf("%d goroutines hanged with ids: %v", len(hanged), hanged)
	}
	log.Println(strings.Repeat("-", 40))
	log.Printf("OPS: %.2f (%d / %.2f)", float64(totalOp)/period.Seconds(), totalOp, period.Seconds())
}

func loadConfig(path string, cfg *hazelcast.Config) error {
	text, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(text, cfg); err != nil {
		return err
	}
	return nil
}

func createClient(configPath string) *hazelcast.Client {
	config := hazelcast.NewConfig()
	if configPath != "" {
		log.Println("Configuration Path : ", configPath)
		if err := loadConfig(configPath, &config); err != nil {
			log.Fatal(err)
		}
	}
	config.SerializationConfig.AddIdentifiedDataSerializableFactory(&Factory{})
	client, err := hazelcast.StartNewClientWithConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func run(ctx context.Context, m *hazelcast.Map, st *Stats) {
	var err error
	for {
		key := strconv.Itoa(rand.Intn(entryCount))
		value := strconv.Itoa(rand.Intn(entryCount))
		op := rand.Intn(100)
		if op < 30 {
			_, err = m.Get(ctx, key)
		} else if op < 60 {
			_, err = m.Put(ctx, key, value)
		} else if op < 80 {
			_, err = m.GetValuesWithPredicate(ctx, predicate.Between("this", 0, 10))
		} else {
			_, err = m.ExecuteOnEntries(ctx, &SimpleEntryProcessor{value: "test"})
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				break
			}
			log.Fatal(err)
		}
		st.IncOpCount()
	}
	log.Printf("Goroutine %d exited", st.Id)
	atomic.AddInt32(&remainingGoroutines, -1)
}

func main() {
	configPath := flag.String("c", "", "Path of the JSON configuration file.")
	durStr := flag.String("d", "48h",
		"Running time. You can use the following prefixes: s -> seconds, m -> minutes, h -> hours and their combinations, e.g., 2h30m")
	flag.Parse()
	dur, err := time.ParseDuration(*durStr)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Duration           : ", dur)
	log.Println("Goroutine Count    : ", goroutineCount)
	client := createClient(*configPath)
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	testMap, err := client.GetMap(ctx, "-test-map")
	if err != nil {
		log.Fatal(err)
	}
	tic = time.Now()
	sts := make([]*Stats, goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		st := &Stats{Id: i}
		sts[i] = st
		go run(ctx, testMap, st)
	}
	go func(ctx context.Context, sts []*Stats) {
		ticker := time.NewTicker(displayDur)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				displayStats(sts)
			case <-ctx.Done():
				return
			}
		}
	}(ctx, sts)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	select {
	case <-ctx.Done():
	case <-c:
	}
	displayStats(sts)
	time.Sleep(10 * time.Second)
	client.Shutdown()
	log.Println(strings.Repeat("*", 40))
	log.Println("Soak test finished with SUCCESS.")
	log.Printf("Remaining goroutine count: %d", remainingGoroutines)
	log.Println(strings.Repeat("-", 40))
}
