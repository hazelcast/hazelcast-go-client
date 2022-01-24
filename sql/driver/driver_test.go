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

package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/sql/driver"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestSetLoggerConfig(t *testing.T) {
	config := &logger.Config{Level: logger.ErrorLevel}
	if err := SetLoggerConfig(config); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, config, driver.LoggerConfig())
	if err := SetLoggerConfig(nil); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, (*logger.Config)(nil), driver.LoggerConfig())
}

func TestSetSSLConfig(t *testing.T) {
	config := &cluster.SSLConfig{Enabled: true}
	if err := SetSSLConfig(config); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, config, driver.SSLConfig())
	if err := SetSSLConfig(nil); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, (*cluster.SSLConfig)(nil), driver.SSLConfig())
}

func TestSetSerializationConfig(t *testing.T) {
	config := &serialization.Config{PortableVersion: 2}
	if err := SetSerializationConfig(config); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, config, driver.SerializationConfig())
	if err := SetSerializationConfig(nil); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, (*serialization.Config)(nil), driver.SerializationConfig())
}
