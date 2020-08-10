// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
	"log"
	"path/filepath"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
)

func main() {
	cfg := hazelcast.NewConfig()

	//Set up group name and password for authentication
	cfg.GroupConfig().SetName("YOUR_CLUSTER_NAME")
	cfg.GroupConfig().SetPassword("YOUR_CLUSTER_PASSWORD")

	// Enable Hazelcast.Cloud configuration and set the token of your cluster.
	discoveryCfg := config.NewCloudConfig()
	discoveryCfg.SetEnabled(true)
	discoveryCfg.SetDiscoveryToken("YOUR_CLUSTER_DISCOVERY_TOKEN")
	cfg.NetworkConfig().SetCloudConfig(discoveryCfg)

	// If you have enabled encryption for your cluster, also configure TLS/SSL for the client.
	// Otherwise, skip this step.
	caFile, _ := filepath.Abs("./ca.pem")
	certFile, _ := filepath.Abs("./cert.pem")
	keyFile, _ := filepath.Abs("./key.pem")
	cfg.NetworkConfig().SSLConfig().SetEnabled(true)
	cfg.NetworkConfig().SSLConfig().SetCaPath(caFile)
	cfg.NetworkConfig().SSLConfig().AddClientCertAndEncryptedKeyPath(certFile, keyFile, "YOUR_KEY_STORE_PASSWORD")
	cfg.NetworkConfig().SSLConfig().ServerName = "SERVER_NAME"

	// Start a new Hazelcast client with this configuration.
	client, _ := hazelcast.NewClientWithConfig(cfg)

	mp, _ := client.GetMap("map-on-the-cloud")
	mp.Put("exampleKey", "examplePut")

	res, _ := mp.Get("exampleKey")

	log.Println("Get returned : ", res)

	client.Shutdown()

}
