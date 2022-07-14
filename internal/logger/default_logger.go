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
	Weight logger.Weight
}

// New returns a Default Logger with defaultLogLevel.
func New() *DefaultLogger {
	l, _ := NewWithLevel(defaultLogLevel) // defaultLogLevel exists, no err
	return l
}

func NewWithLevel(loggingLevel logger.Level) (*DefaultLogger, error) {
	numericLevel, err := logger.WeightForLogLevel(loggingLevel)
	if err != nil {
		return nil, err
	}
	return &DefaultLogger{
		Logger: log.New(os.Stderr, "", log.LstdFlags),
		Weight: numericLevel,
	}, nil
}

func (l *DefaultLogger) Log(wantedLevel logger.Weight, formatter func() string) {
	if l.Weight < wantedLevel {
		return
	}
	var logLevel logger.Level
	switch wantedLevel {
	case logger.WeightTrace:
		logLevel = logger.TraceLevel
	case logger.WeightDebug:
		logLevel = logger.DebugLevel
	case logger.WeightInfo:
		logLevel = logger.InfoLevel
	case logger.WeightWarn:
		logLevel = logger.WarnLevel
	case logger.WeightError:
		logLevel = logger.ErrorLevel
	case logger.WeightFatal:
		logLevel = logger.FatalLevel
	case logger.WeightOff:
		logLevel = logger.OffLevel
	default:
		return // unknown level, do not log anything
	}

	s := fmt.Sprintf("%-5s: %s", strings.ToUpper(logLevel.String()), formatter())
	_ = l.Output(logCallDepth, s) // don't have retry mechanism in case writing to buffer fails
}

// LogAdaptor is used to convert logger implementations of public interface logger.LogAdaptor to internal logging interface LogAdaptor
type LogAdaptor struct {
	logger.Logger
}

// Debug runs the given function to generate the logger string, if logger level is debug or finer.
func (la LogAdaptor) Debug(f func() string) {
	la.Log(logger.WeightDebug, f)
}

// Trace runs the given function to generate the logger string, if logger level is trace or finer.
func (la LogAdaptor) Trace(f func() string) {
	la.Log(logger.WeightTrace, f)
}

// TraceHere logs the function name, source file and line number of the call site.
func (la LogAdaptor) TraceHere() {
	const pkg = "github.com/hazelcast/hazelcast-go-client/"
	const pkgLen = len(pkg)
	la.Log(logger.WeightTrace, func() string {
		pc, file, line, ok := runtime.Caller(3)
		if ok {
			if details := runtime.FuncForPC(pc); details != nil {
				fun := details.Name()[pkgLen:]
				return fmt.Sprintf("HERE -> %s [%s:%d]", fun, file, line)
			}
		}
		return fmt.Sprintf("(could not generate TraceHere output)")
	})
}

// Info runs the given function to generate the logger string, if logger level is trace or finer.
func (la LogAdaptor) Info(f func() string) {
	la.Log(logger.WeightInfo, f)
}

// Infof formats the given string with the given values, if logger level is info or finer.
func (la LogAdaptor) Infof(format string, values ...interface{}) {
	la.Log(logger.WeightInfo, func() string {
		return fmt.Sprintf(format, values...)
	})
}

// Warnf formats the given string with the given values, if logger level is warn or finer.
func (la LogAdaptor) Warnf(format string, values ...interface{}) {
	la.Log(logger.WeightWarn, func() string {
		return fmt.Sprintf(format, values...)
	})
}

// Errorf formats the given string with the given values, if logger level is error or finer.
func (la LogAdaptor) Errorf(format string, values ...interface{}) {
	la.Log(logger.WeightError, func() string {
		return fmt.Errorf(format, values...).Error()
	})
}
