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
	"testing"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

func TestSSLConfig_SetCAPath(t *testing.T) {
	sslConfig := cluster.SSLConfig{
		Enabled: true,
	}
	err := sslConfig.SetCAPath("invalid-filepath")
	if !errors.Is(err, hzerrors.NewIOError(err.Error(), err)) {
		t.Fatal("error types does not match")
	}
}
