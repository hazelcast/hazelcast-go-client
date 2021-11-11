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
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/hazelcast/hazelcast-go-client/logger"
)

const (
	// logCallDepth is used for removing the last two method names from call trace when logging file names.
	logCallDepth    = 3
	defaultLogLevel = logger.InfoLevel
	tracePrefix     = "TRACE"
	warnPrefix      = "WARN"
	debugPrefix     = "DEBUG"
	errorPrefix     = "ERROR"
	infoPrefix      = "INFO"
)

// DefaultLogger has Go's built-in logger embedded in it. It adds level logging.
// To set the logging level, one should use the LoggingLevel property. For example
// to set it to debug level:
//  config.SetProperty(property.LoggingLevel.Name(), logger.DebugLevel)
// If loggerConfig.SetLogger() method is called, the LoggingLevel property will not be used.
type DefaultLogger struct {
	*log.Logger
	Level int
}

// New returns a Default Logger with defaultLogLevel.
func New() *DefaultLogger {
	l, _ := NewWithLevel(defaultLogLevel) // defaultLogLevel exists, no err
	return l
}

func NewWithLevel(loggingLevel logger.Level) (*DefaultLogger, error) {
	numericLevel, err := logger.GetLogLevel(loggingLevel)
	if err != nil {
		return nil, err
	}
	return &DefaultLogger{
		Logger: log.New(os.Stderr, "", log.LstdFlags),
		Level:  numericLevel,
	}, nil
}

func (l *DefaultLogger) Log(level logger.Level, formatter func() string) {
	wantedLevel, err := logger.GetLogLevel(level)
	if err != nil {
		return
	}
	if l.Level < wantedLevel {
		return
	}
	s := fmt.Sprintf("%-5s: %s", strings.ToUpper(level.String()), formatter())
	_ = l.Output(logCallDepth, s) // don't have retry mechanism in case writing to buffer fails
}

func (l *DefaultLogger) findCallerFuncName() string {
	pc, _, _, _ := runtime.Caller(logCallDepth)
	return runtime.FuncForPC(pc).Name()
}

// LogAdaptor is used to convert logger implementations of public interface logger.LogAdaptor to internal logging interface LogAdaptor
type LogAdaptor struct {
	logger.Logger
}

// Debug runs the given function to generate the logger string, if logger level is debug or finer.
func (la LogAdaptor) Debug(f func() string) {
	la.Log(logger.DebugLevel, f)
}

// Trace runs the given function to generate the logger string, if logger level is trace or finer.
func (la LogAdaptor) Trace(f func() string) {
	la.Log(logger.TraceLevel, f)
}

// Infof formats the given string with the given values, if logger level is info or finer.
func (la LogAdaptor) Infof(format string, values ...interface{}) {
	la.Log(logger.InfoLevel, func() string {
		return fmt.Sprintf(format, values...)
	})
}

// Warnf formats the given string with the given values, if logger level is warn or finer.
func (la LogAdaptor) Warnf(format string, values ...interface{}) {
	la.Log(logger.WarnLevel, func() string {
		return fmt.Sprintf(format, values...)
	})
}

// Errorf formats the given string with the given values, if logger level is error or finer.
func (la LogAdaptor) Errorf(format string, values ...interface{}) {
	la.Log(logger.ErrorLevel, func() string {
		return fmt.Errorf(format, values...).Error()
	})
}
