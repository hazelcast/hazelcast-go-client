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

go vet ./... 2>&1 | \
  grep -v "should have signature WriteByte(byte) error" | \
  grep -v "should have signature ReadByte() (byte, error)" | \
  grep -v "# github.com/hazelcast/hazelcast-go-client/serialization" | \
  grep -v "# github.com/hazelcast/hazelcast-go-client/internal/serialization" \
  || true

staticcheck ./...

# Ensure fields are optimally aligned
# From: https://pkg.go.dev/golang.org/x/tools@v0.1.0/go/analysis/passes/fieldalignment
fieldalignment ./...