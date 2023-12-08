/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	ctx := context.Background()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		panic(fmt.Errorf("starting the client: %w", err))
	}
	m, err := client.CPSubsystem().GetMap(ctx, "map@group")
	if err != nil {
		panic(fmt.Errorf("getting CPMap: %w", err))
	}
	err = m.Set(ctx, "key", "value")
	if err != nil {
		panic(fmt.Errorf("setting the key: %w", err))
	}
	v, err := m.Get(ctx, "key")
	if err != nil {
		panic(fmt.Errorf("getting the key: %w", err))
	}
	fmt.Println("OK", v)
}
