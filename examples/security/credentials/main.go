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

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	ctx := context.TODO()
	config := hazelcast.Config{}
	cc := &config.Cluster
	cc.Network.SetAddresses("192.168.1.101:5701")
	cc.Name = "hello-world"
	creds := &cc.Security.Credentials
	creds.Username = "member1"
	creds.Password = "s3crEt"
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		panic(err)
	}
	client.Shutdown(ctx)
}
