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

// DefaultLogger has Go's built-in log embedded in it. It adds level logging.
// To set the logging level, one should use the LoggingLevel property. For example
// to set it to debug level:
//  config.SetProperty(property.LoggingLevel.Name(), logger.DebugLevel)
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

func (l *DefaultLogger) CanLog(level logger.Level) bool {
	numericLevel, err := logger.GetLogLevel(level)
	if err != nil {
		return false
	}
	loggerLevel, err := logger.GetLogLevel(l.Level)
	if err != nil {
		fmt.Println("logger has invalid log level, will not log something useful")
		return false
	}
	return loggerLevel >= numericLevel
}

// Debug logs the given arguments at debug level if the level is greater than or equal to debug level.
func (l *DefaultLogger) Debug(f func() string) {
	if l.CanLog(logger.DebugLevel) && f != nil {
		s := fmt.Sprintf("DEBUG: %s", f())
		l.Output(logCallDepth, s)
	}
}

func (l *DefaultLogger) Trace(f func() string) {
	if l.CanLog(logger.TraceLevel) && f != nil {
		s := fmt.Sprintf("TRACE: %s", f())
		l.Output(logCallDepth, s)
	}
}

// Info logs the given arguments at info level if the level is greater than or equal to info level.
func (l *DefaultLogger) Info(args ...interface{}) {
	if l.CanLog(logger.InfoLevel) {
		callerName := l.findCallerFuncName()
		s := callerName + "\n" + infoPrefix + ": " + fmt.Sprint(args...)
		l.Output(logCallDepth, s)
	}
}

func (l *DefaultLogger) Infof(format string, values ...interface{}) {
	if l.CanLog(logger.InfoLevel) {
		s := fmt.Sprintf("INFO : %s", fmt.Sprintf(format, values...))
		l.Output(logCallDepth, s)
	}
}

// Warn logs the given arguments at warn level if the level is greater than or equal to warn level.
func (l *DefaultLogger) Warn(args ...interface{}) {
	if l.CanLog(logger.WarnLevel) {
		callerName := l.findCallerFuncName()
		s := callerName + "\n" + warnPrefix + ": " + fmt.Sprint(args...)
		l.Output(logCallDepth, s)
	}
}

func (l *DefaultLogger) Warnf(format string, values ...interface{}) {
	if l.CanLog(logger.WarnLevel) {
		s := fmt.Sprintf("WARN : %s", fmt.Sprintf(format, values...))
		l.Output(logCallDepth, s)
	}
}

// Error logs the given arguments at error level if the level is greater than or equal to error level.
func (l *DefaultLogger) Error(err error) {
	l.Errorf(err.Error())
}

func (l *DefaultLogger) Errorf(format string, values ...interface{}) {
	if l.CanLog(logger.ErrorLevel) {
		s := fmt.Sprintf("ERROR: %s", fmt.Errorf(format, values...).Error())
		l.Output(logCallDepth, s)
	}
}

func (l *DefaultLogger) findCallerFuncName() string {
	pc, _, _, _ := runtime.Caller(logCallDepth)
	return runtime.FuncForPC(pc).Name()
}
