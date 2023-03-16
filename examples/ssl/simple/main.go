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

// For this code sample, use the provided hazelcast.xml.
// And make sure server.keystore and server.truststore is accessible by the server,

func main() {
	// Hazelcast server should be started with SSL enabled to use SSLConfig
	ctx := context.TODO()
	var cfg hazelcast.Config
	cfg.Cluster.Network.SetAddresses("localhost:5701")
	ssl := &cfg.Cluster.Network.SSL
	// TLS/SSL is enabled
	ssl.Enabled = true
	// we set insecureSkipVerify, since the sample certificates for this example were self-signed.
	// do not enable it in production.
	ssl.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	// Set the server name and select the protocol used in SSL communication, default is TLSv1_2
	ssl.ServerName = "test.hazelcast.com"
	// Absolute paths of PEM files must be given
	err := ssl.SetCAPath("examples/ssl/etc/ca.crt")
	if err != nil {
		return
	}
	// Start a new Hazelcast client with SSL configuration.
	client, err := hazelcast.StartNewClientWithConfig(ctx, cfg)
	if err != nil {
		panic(err)
	}
	fmt.Println("Connection Successful!")
	if err := client.Shutdown(ctx); err != nil {
		panic(fmt.Errorf("shutting down the client: %w", err))
	}
}
