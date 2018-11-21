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

package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsValidLogLevel(t *testing.T) {
	logLevel := "debug"
	isValid := isValidLogLevel(logLevel)
	assert.True(t, isValid)
}

func TestIsValidLogLevelCaseInsensitive(t *testing.T) {
	logLevel := "deBUg"
	isValid := isValidLogLevel(logLevel)
	assert.True(t, isValid)
}

func TestIsValidLogLevelInvalidLevel(t *testing.T) {
	logLevel := "deb"
	isValid := isValidLogLevel(logLevel)
	assert.False(t, isValid)
}

func TestGetLogLevel(t *testing.T) {
	logLevel := "panic"
	level, err := GetLogLevel(logLevel)
	assert.NoError(t, err)
	assert.Equal(t, level, PanicLevel)
}

func TestGetLogLevelError(t *testing.T) {
	logLevel := "p"
	_, err := GetLogLevel(logLevel)
	assert.Error(t, err)
}
