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
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/logger"
)

const logMessage = "dummy"

func createWithLevelAndLog(level logger.Level) string {
	dl := New()
	dl.Level = level
	buf := new(bytes.Buffer)
	dl.SetOutput(buf)
	l := LogAdaptor{dl}
	l.Debug(func() string { return logMessage })
	l.Trace(func() string { return logMessage })
	l.Warnf(logMessage)
	l.Infof(logMessage)
	l.Errorf(logMessage)
	return buf.String()
}

func TestDefaultLogger_TraceLevel(t *testing.T) {
	loggedMessages := createWithLevelAndLog(logger.TraceLevel)
	assert.Contains(t, loggedMessages, tracePrefix)
	assert.Contains(t, loggedMessages, debugPrefix)
	assert.Contains(t, loggedMessages, warnPrefix)
	assert.Contains(t, loggedMessages, errorPrefix)
	assert.Contains(t, loggedMessages, infoPrefix)
}

func TestDefaultLogger_DebugLevel(t *testing.T) {
	loggedMessages := createWithLevelAndLog(logger.DebugLevel)
	assert.NotContains(t, loggedMessages, tracePrefix)
	assert.Contains(t, loggedMessages, debugPrefix)
	assert.Contains(t, loggedMessages, warnPrefix)
	assert.Contains(t, loggedMessages, errorPrefix)
	assert.Contains(t, loggedMessages, infoPrefix)
}

func TestDefaultLogger_WarnLevel(t *testing.T) {
	loggedMessages := createWithLevelAndLog(logger.WarnLevel)
	assert.NotContains(t, loggedMessages, tracePrefix)
	assert.NotContains(t, loggedMessages, debugPrefix)
	assert.NotContains(t, loggedMessages, infoPrefix)
	assert.Contains(t, loggedMessages, warnPrefix)
	assert.Contains(t, loggedMessages, errorPrefix)
}

func TestDefaultLogger_InfoLevel(t *testing.T) {
	loggedMessages := createWithLevelAndLog(logger.InfoLevel)
	assert.NotContains(t, loggedMessages, tracePrefix)
	assert.NotContains(t, loggedMessages, debugPrefix)
	assert.Contains(t, loggedMessages, infoPrefix)
	assert.Contains(t, loggedMessages, warnPrefix)
	assert.Contains(t, loggedMessages, errorPrefix)
}

func TestDefaultLogger_ErrorLevel(t *testing.T) {
	loggedMessages := createWithLevelAndLog(logger.ErrorLevel)
	assert.NotContains(t, loggedMessages, tracePrefix)
	assert.NotContains(t, loggedMessages, debugPrefix)
	assert.NotContains(t, loggedMessages, warnPrefix)
	assert.NotContains(t, loggedMessages, infoPrefix)
	assert.Contains(t, loggedMessages, errorPrefix)
}
