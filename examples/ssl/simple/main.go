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
	"crypto/tls"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	// Hazelcast server should be started with SSL enabled to use SSLConfig
	ctx := context.TODO()
	config := hazelcast.NewConfig()
	config.Cluster.Network.SetAddresses("foo.bar.com:8888")
	// TLS/SSL is enabled
	config.Cluster.Network.SSL.Enabled = true
	// Absolute paths of PEM files must be given
	err := config.Cluster.Network.SSL.SetCAPath("/path/of/server.pem")
	if err != nil {
		return
	}
	// Set the server name and select the protocol used in SSL communication, default is TLSv1_2
	config.Cluster.Network.SSL.SetTLSConfig(&tls.Config{ServerName: "foo.bar", MinVersion: tls.VersionTLS13})
	// Start a new Hazelcast client with SSL configuration.
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)
	fmt.Println("Connection Successful!")
}
