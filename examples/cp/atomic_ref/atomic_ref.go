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
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		panic(err)
	}
	ar, err := client.CPSubsystem().GetAtomicRef(ctx, "myref@group")
	if err != nil {
		panic(err)
	}
	v, err := ar.Get(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("VALUE", v)
}
