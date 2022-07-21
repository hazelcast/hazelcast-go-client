/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/hazelcast/hazelcast-go-client"
)

const (
	defaultKeyCount       = 1_000
	defaultRepeat         = 10_000
	defaultGoroutineCount = 1
)

type Config struct {
	MapName        string
	KeyCount       int
	Repeat         int
	GoroutineCount int
	Warmup         bool
	Client         hazelcast.Config
}

func (c *Config) LoadFromPath(path string) error {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("path not found: %s: %w", path, err)
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading file: %s: %w", path, err)
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return fmt.Errorf("unmarshalling config: %w", err)
	}
	if c.KeyCount == 0 {
		c.KeyCount = defaultKeyCount
	}
	if c.Repeat == 0 {
		c.Repeat = defaultRepeat
	}
	if c.GoroutineCount == 0 {
		c.GoroutineCount = defaultGoroutineCount
	}
	return nil
}
