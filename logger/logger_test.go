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

package logger

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
)

func TestGetLogLevel(t *testing.T) {
	logLevels := []struct {
		level    string
		levelInt Weight
	}{
		{"error", WeightError},
		{"trace", WeightTrace},
		{"off", WeightOff},
		{"fatal", WeightFatal},
		{"warn", WeightWarn},
		{"debug", WeightDebug},
		{"info", WeightInfo},
	}
	for _, logLevel := range logLevels {
		level, err := WeightForLogLevel(Level(logLevel.level))
		assert.NoError(t, err)
		assert.Equal(t, level, logLevel.levelInt)
	}
}

func TestGetLogLevelError(t *testing.T) {
	logLevel := "p"
	_, err := WeightForLogLevel(Level(logLevel))
	assert.Error(t, err)
}

func TestLogLevelWithCustomLoggerFails(t *testing.T) {
	cfg := Config{}
	cfg.Level = InfoLevel
	cfg.CustomLogger = &customLogger{}
	err := cfg.Validate()
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, hzerrors.ErrIllegalArgument))
}

type customLogger struct{}

func (c customLogger) Log(weight Weight, f func() string) {}
