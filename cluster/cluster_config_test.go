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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
)

func TestConfig_Validate(t *testing.T) {
	testCases := []struct {
		msg    string
		config cluster.Config
	}{
		{config: cluster.Config{}, msg: "invalid cluster name: illegal argument error"},
		{config: cluster.Config{Name: "x", ConnectionTimeout: -10 * time.Second}, msg: "invalid connection timeout: illegal argument error"},
		{config: cluster.Config{Name: "x", HeartbeatInterval: -10 * time.Second}, msg: "invalid heartbeat interval: illegal argument error"},
		{config: cluster.Config{Name: "x", HeartbeatTimeout: -10 * time.Second}, msg: "invalid heartbeat timeout: illegal argument error"},
		{config: cluster.Config{Name: "x", InvocationTimeout: -10 * time.Second}, msg: "invalid invocation timeout: illegal argument error"},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("validate %d", i), func(t *testing.T) {
			err := tc.config.Validate()
			if !errors.Is(err, hzerrors.ErrIllegalArgument) {
				t.Fatalf("expected illegal argument error")
			}
			assert.Equal(t, tc.msg, err.Error())
		})
	}
	t.Run("validate address", func(t *testing.T) {
		config := cluster.Config{Name: "x", Addrs: []string{"foo:"}}
		if err := config.Validate(); err == nil {
			t.Fatal("should have failed")
		}
	})
}
