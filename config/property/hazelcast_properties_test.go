// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package property

import (
	"testing"

	"os"

	"time"

	"github.com/hazelcast/hazelcast-go-client/config"
)

func TestHazelcastProperties_GetStringWithConfig(t *testing.T) {
	cfg := config.New()

	property := NewHazelcastPropertyString("testName", "testDefaultValue")

	cfg.SetProperty("testName", "testValue")

	properties := NewHazelcastProperties(cfg.Properties())

	if value := properties.GetString(property); value != "testValue" {
		t.Errorf("expected %s got %s", "testValue", value)
	}

}

func TestHazelcastProperties_GetStringWithDefaultValue(t *testing.T) {
	cfg := config.New()
	expected := "testDefaultValue"
	property := NewHazelcastPropertyString("testName", expected)

	properties := NewHazelcastProperties(cfg.Properties())

	if value := properties.GetString(property); value != expected {
		t.Errorf("expected %s got %s", expected, value)
	}

}

func TestHazelcastProperties_GetStringWithEnvironmentVariable(t *testing.T) {
	cfg := config.New()

	expected := "testValue"
	property := NewHazelcastPropertyString("testName", "testDefaultValue")

	os.Setenv("testName", expected)
	properties := NewHazelcastProperties(cfg.Properties())

	if value := properties.GetString(property); value != expected {
		t.Errorf("expected %s got %s", expected, value)
	}

}

func TestHazelcastProperties_GetStringConfigAndEnvironmentBothSet(t *testing.T) {
	cfg := config.New()

	expected := "testValue"
	name := "testName"
	property := NewHazelcastPropertyString(name, "testDefaultValue")

	os.Setenv(name, "testEnvVal")

	cfg.SetProperty(name, expected)
	properties := NewHazelcastProperties(cfg.Properties())

	if value := properties.GetString(property); value != expected {
		t.Errorf("expected %s got %s", expected, value)
	}

}

func TestHazelcastProperties_GetBoolean(t *testing.T) {
	name := "testBoolName"

	property := NewHazelcastPropertyBool(name, true)
	properties := NewHazelcastProperties(nil)

	if value := properties.GetBoolean(property); !value {
		t.Errorf("expected true for %s", name)
	}

	cfg := config.New()
	cfg.SetProperty(name, "false")

	properties = NewHazelcastProperties(cfg.Properties())
	if value := properties.GetBoolean(property); value {
		t.Errorf("expected false for %s", name)
	}

}

func TestHazelcastProperties_GetDuration(t *testing.T) {
	name := "testDuration"
	coeff := int64(5)
	timeUnit := time.Second
	duration := time.Duration(coeff) * timeUnit

	property := NewHazelcastPropertyInt64WithTimeUnit(name, coeff, timeUnit)
	properties := NewHazelcastProperties(nil)

	if value := properties.GetDuration(property); value != duration {
		t.Errorf("expected %s got %s", duration, value)
	}

}

func TestHazelcastProperties_GetDurationFromEnv(t *testing.T) {
	name := "testDuration"
	timeUnit := time.Millisecond
	os.Setenv(name, "300")
	property := NewHazelcastPropertyInt64WithTimeUnit(name, 500, timeUnit)
	properties := NewHazelcastProperties(nil)
	expected := time.Duration(300) * timeUnit
	if value := properties.GetDuration(property); value != expected {
		t.Errorf("expected %s got %s", expected, value)
	}

}

func TestHazelcastProperties_GetDurationFromEnvWhenIncorrectFormat(t *testing.T) {
	name := "testDuration"
	coeff := int64(5)
	timeUnit := time.Second
	duration := time.Duration(coeff) * timeUnit

	os.Setenv(name, "AAA300")
	property := NewHazelcastPropertyInt64WithTimeUnit(name, coeff, timeUnit)
	properties := NewHazelcastProperties(nil)
	if value := properties.GetDuration(property); value != duration {
		t.Errorf("expected %s got %s", duration, value)
	}
}

func TestHazelcastProperties_GetPositiveDurationFromEnv(t *testing.T) {
	name := "testDuration"
	coeff := int64(5)
	timeUnit := time.Second
	duration := time.Duration(coeff) * timeUnit
	os.Setenv(name, "-300")
	property := NewHazelcastPropertyInt64WithTimeUnit(name, coeff, timeUnit)
	properties := NewHazelcastProperties(nil)
	if value := properties.GetPositiveDuration(property); value != duration {
		t.Errorf("expected %s got %s", duration, value)
	}

}
