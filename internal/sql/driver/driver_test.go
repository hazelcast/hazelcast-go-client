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

package driver_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/sql/driver"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestParseDSN(t *testing.T) {
	testCases := []struct {
		Cluster *cluster.Config
		DSN     string
	}{
		{
			DSN: "",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"127.0.0.1:5701"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
				},
			},
		},
		{
			DSN: "10.20.30.40:5000",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"10.20.30.40:5000"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
				},
			},
		},
		{
			DSN: "10.20.30.40:5000,11.21.31.41:5001",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"10.20.30.40:5000", "11.21.31.41:5001"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
				},
			},
		},
		{
			DSN: "10.20.30.40:5000;ClusterName=my-cluster",
			Cluster: &cluster.Config{
				Name: "my-cluster",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"10.20.30.40:5000"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.DSN, func(t *testing.T) {
			tc.Cluster.Validate()
			c, err := driver.ParseDSN(tc.DSN)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.Cluster, c.Cluster)
		})
	}
}
