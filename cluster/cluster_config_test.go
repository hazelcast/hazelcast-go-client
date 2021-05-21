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

package cluster_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/cluster"
)

func TestConfig_Validate(t *testing.T) {
	testCases := []struct {
		err    error
		config cluster.Config
	}{
		{config: cluster.Config{}, err: cluster.ErrConfigInvalidClusterName},
		{config: cluster.Config{Name: "x", ConnectionTimeout: -10 * time.Second}, err: cluster.ErrConfigInvalidConnectionTimeout},
		{config: cluster.Config{Name: "x", HeartbeatInterval: -10 * time.Second}, err: cluster.ErrConfigInvalidHeartbeatInterval},
		{config: cluster.Config{Name: "x", HeartbeatTimeout: -10 * time.Second}, err: cluster.ErrConfigInvalidHeartbeatTimeout},
		{config: cluster.Config{Name: "x", InvocationTimeout: -10 * time.Second}, err: cluster.ErrConfigInvalidInvocationTimeout},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("validate %d", i), func(t *testing.T) {
			assert.Equal(t, tc.err, tc.config.Validate())
		})
	}
	t.Run("validate address", func(t *testing.T) {
		config := cluster.Config{Name: "x", Addrs: []string{"foo:"}}
		if err := config.Validate(); err == nil {
			t.Fatal("should have failed")
		}
	})
}
