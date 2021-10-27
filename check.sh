#!/bin/bash
#
# Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Runs various linters

# Note that we filter out
# - should have signature WriteByte(byte) error
# - should have signature ReadByte() (byte, error)
# And the following packages which contain the functions above:
# - github.com/hazelcast/hazelcast-go-client/serialization
# - github.com/hazelcast/hazelcast-go-client/internal/serialization
# And exclude any files with "org-website" in the path because these
# are examples

go vet -tags hazelcastinternal ./... 2>&1 | \
  grep -v "should have signature WriteByte(byte) error" | \
  grep -v "should have signature ReadByte() (byte, error)" | \
  grep -v "# github.com/hazelcast/hazelcast-go-client/serialization" | \
  grep -v "# github.com/hazelcast/hazelcast-go-client/internal/serialization" | \
  grep -v "org-website" \
  || true

staticcheck -tags hazelcastinternal $(go list ./... | grep -v org-website)

# Ensure fields are optimally aligned
# From: https://pkg.go.dev/golang.org/x/tools@v0.1.0/go/analysis/passes/fieldalignment
# If missing install via: go get -u golang.org/x/tools/...
# Structs in following files should not be sorted due to: https://pkg.go.dev/sync/atomic#pkg-note-BUG
fieldalignment -tags $(go list ./... | grep -v org-website) 2>&1 | \
  grep -v "internal/cluster/connection_manager.go" | \
  grep -v "internal/cluster/view_listener_service.go" | \
  grep -v "flake_id_generator.go" \
  || true