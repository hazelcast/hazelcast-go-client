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

	logger "github.com/hazelcast/hazelcast-go-client/log"
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

// DefaultLogger has Go's built-in log embedded in it. It adds level logging.
// To set the logging level, one should use the LoggingLevel property. For example
// to set it to debug level:
//  config.SetProperty(property.LoggingLevel.Name(), log.DebugLevel)
// If loggerConfig.SetLogger() method is called, the LoggingLevel property will not be used.
type DefaultLogger struct {
	*log.Logger
	Level logger.Level
}

// New returns a Default Logger with defaultLogLevel.
func New() *DefaultLogger {
	return &DefaultLogger{
		Logger: log.New(os.Stderr, "", log.LstdFlags),
		Level:  defaultLogLevel,
	}
}

func NewWithLevel(loggingLevel logger.Level) *DefaultLogger {
	return &DefaultLogger{
		Logger: log.New(os.Stderr, "", log.LstdFlags),
		Level:  loggingLevel,
	}
}

func (l *DefaultLogger) canLog(level logger.Level) bool {
	numericLevel, err := logger.GetLogLevel(level)
	if err != nil {
		return false
	}
	loggerLevel, err := logger.GetLogLevel(l.Level)
	if err != nil {
		fmt.Println("log has invalid log level, will not log something useful")
		return false
	}
	return loggerLevel >= numericLevel
}

func (l *DefaultLogger) Log(level logger.Level, formatter func() string) {
	if !l.canLog(logger.DebugLevel) || formatter == nil {
		return
	}
	s := fmt.Sprintf("%s: %s", strings.ToUpper(level.String()), formatter())
	_ = l.Output(logCallDepth, s)
}

func (l *DefaultLogger) findCallerFuncName() string {
	pc, _, _, _ := runtime.Caller(logCallDepth)
	return runtime.FuncForPC(pc).Name()
}

// LogAdaptor is used to convert log implementations of public interface logger.Logger to internal logging interface Logger
type LogAdaptor struct {
	logger.Logger
}

// compile time check for interface satisfaction
var _ Logger = LogAdaptor{}

// Debug runs the given function to generate the log string, if log level is debug or finer.
func (la LogAdaptor) Debug(f func() string) {
	la.Log(logger.DebugLevel, f)
}

// Trace runs the given function to generate the log string, if log level is trace or finer.
func (la LogAdaptor) Trace(f func() string) {
	la.Log(logger.TraceLevel, f)
}

// Info runs the given function to generate the log string, if log level is info or finer.
func (la LogAdaptor) Info(f func() string) {
	la.Log(logger.InfoLevel, f)
}

// Infof formats the given string with the given values, if log level is info or finer.
func (la LogAdaptor) Infof(format string, values ...interface{}) {
	la.Log(logger.InfoLevel, func() string {
		return fmt.Sprintf(format, values...)
	})
}

// Warn formats the given string with the given values, if log level is warn or finer.
func (la LogAdaptor) Warn(f func() string) {
	la.Log(logger.WarnLevel, f)
}

// Warnf formats the given string with the given values, if log level is warn or finer.
func (la LogAdaptor) Warnf(format string, values ...interface{}) {
	la.Log(logger.WarnLevel, func() string {
		return fmt.Sprintf(format, values...)
	})
}

// Error logs the given args at error level.
func (la LogAdaptor) Error(err error) {
	la.Log(logger.ErrorLevel, func() string {
		return err.Error()
	})
}

// Errorf formats the given string with the given values, if log level is error or finer.
func (la LogAdaptor) Errorf(format string, values ...interface{}) {
	la.Log(logger.ErrorLevel, func() string {
		return fmt.Sprintf(format, values...)
	})
}

// Logger is the interface that is used internally by client for logging.
type Logger interface {
	// Debug logs the given arg at debug level.
	Debug(f func() string)
	// Trace logs the given arg at trace level.
	Trace(f func() string)
	// Infof logs the given args at info level.
	Infof(format string, values ...interface{})
	// Warnf logs the given args at warn level.
	Warnf(format string, values ...interface{})
	// Error logs the given args at error level.
	Error(err error)
	// Errorf logs the given args at error level with the given format
	Errorf(format string, values ...interface{})
}
