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
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

var httpClient = &http.Client{}

func fetchAndSave(ctx context.Context, m *hazelcast.Map, url string) error {
	request, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	// handle the response
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if err := m.Set(ctx, url, serialization.JSON(b)); err != nil {
		return err
	}

	// get back the response
	content, err := m.Get(ctx, url)
	if err != nil {
		return err
	}
	stringContent := string(content.(serialization.JSON))

	// print it!
	fmt.Println("URL", url)
	fmt.Println(stringContent)
	return nil
}

func main() {
	// create and start the Hazelcast client
	ctx := context.Background()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	m, err := client.GetMap(ctx, "foo")
	if err != nil {
		log.Fatal(err)
	}

	// create the context
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// scrape some JSON documents
	for i := 0; i < 42; i++ {
		url := fmt.Sprintf("https://httpbin.org/get?a=%d", i)
		if err := fetchAndSave(ctx, m, url); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			panic(err)
		}
		time.Sleep(1 * time.Second)
	}

	// shutdown the client to release resources
	client.Shutdown(ctx)
}
