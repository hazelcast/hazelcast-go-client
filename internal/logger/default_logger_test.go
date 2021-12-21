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

func createWithLevelAndLog(level logger.Level) (string, error) {
	var err error
	dl := New()
	dl.Weight, err = logger.GetLogLevel(level)
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	dl.SetOutput(buf)
	l := LogAdaptor{dl}
	l.Debug(func() string { return logMessage })
	l.Trace(func() string { return logMessage })
	l.Warnf(logMessage)
	l.Infof(logMessage)
	l.Errorf(logMessage)
	return buf.String(), nil
}

func TestDefaultLogger_TraceLevel(t *testing.T) {
	loggedMessages, err := createWithLevelAndLog(logger.TraceLevel)
	if err != nil {
		t.Fatal("invalid log level")
	}
	assert.Contains(t, loggedMessages, tracePrefix)
	assert.Contains(t, loggedMessages, debugPrefix)
	assert.Contains(t, loggedMessages, warnPrefix)
	assert.Contains(t, loggedMessages, errorPrefix)
	assert.Contains(t, loggedMessages, infoPrefix)
}

func TestDefaultLogger_DebugLevel(t *testing.T) {
	loggedMessages, err := createWithLevelAndLog(logger.DebugLevel)
	if err != nil {
		t.Fatal("invalid log level")
	}
	assert.NotContains(t, loggedMessages, tracePrefix)
	assert.Contains(t, loggedMessages, debugPrefix)
	assert.Contains(t, loggedMessages, warnPrefix)
	assert.Contains(t, loggedMessages, errorPrefix)
	assert.Contains(t, loggedMessages, infoPrefix)
}

func TestDefaultLogger_WarnLevel(t *testing.T) {
	loggedMessages, err := createWithLevelAndLog(logger.WarnLevel)
	if err != nil {
		t.Fatal("invalid log level")
	}
	assert.NotContains(t, loggedMessages, tracePrefix)
	assert.NotContains(t, loggedMessages, debugPrefix)
	assert.NotContains(t, loggedMessages, infoPrefix)
	assert.Contains(t, loggedMessages, warnPrefix)
	assert.Contains(t, loggedMessages, errorPrefix)
}

func TestDefaultLogger_InfoLevel(t *testing.T) {
	loggedMessages, err := createWithLevelAndLog(logger.InfoLevel)
	if err != nil {
		t.Fatal("invalid log level")
	}
	assert.NotContains(t, loggedMessages, tracePrefix)
	assert.NotContains(t, loggedMessages, debugPrefix)
	assert.Contains(t, loggedMessages, infoPrefix)
	assert.Contains(t, loggedMessages, warnPrefix)
	assert.Contains(t, loggedMessages, errorPrefix)
}

func TestDefaultLogger_ErrorLevel(t *testing.T) {
	loggedMessages, err := createWithLevelAndLog(logger.ErrorLevel)
	if err != nil {
		t.Fatal("invalid log level")
	}
	assert.NotContains(t, loggedMessages, tracePrefix)
	assert.NotContains(t, loggedMessages, debugPrefix)
	assert.NotContains(t, loggedMessages, warnPrefix)
	assert.NotContains(t, loggedMessages, infoPrefix)
	assert.Contains(t, loggedMessages, errorPrefix)
}
