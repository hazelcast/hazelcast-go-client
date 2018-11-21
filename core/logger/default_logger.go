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
	"fmt"
	"log"
	"os"
)

const (
	// logCallDepth is used for removing the last two method names from call trace when logging file names.
	logCallDepth    = 2
	defaultLogLevel = InfoLevel
)

// DefaultLogger has Go's built in log embedded in it. It adds level logging.
type DefaultLogger struct {
	*log.Logger
	Level int
}

// New returns a Default Logger with defaultLogLevel.
func New() *DefaultLogger {
	return &DefaultLogger{
		Logger: log.New(os.Stderr, "", log.LstdFlags),
		Level:  defaultLogLevel,
	}
}

// Debug logs the given arguments at debug level if the level is greater than or equal to debug level.
func (l *DefaultLogger) Debug(args ...interface{}) {
	if l.canLogDebug() {
		s := "[debug] " + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
	}
}

// Trace logs the given arguments at trace level if the level is greater than or equal to trace level.
func (l *DefaultLogger) Trace(args ...interface{}) {
	if l.canLogTrace() {
		s := "[trace] " + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
	}
}

// Info logs the given arguments at info level if the level is greater than or equal to info level.
func (l *DefaultLogger) Info(args ...interface{}) {
	if l.canLogInfo() {
		s := "[info] " + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
	}
}

// Warn logs the given arguments at warn level if the level is greater than or equal to warn level.
func (l *DefaultLogger) Warn(args ...interface{}) {
	if l.canLogWarn() {
		s := "[warn] " + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
	}
}

// Error logs the given arguments at error level if the level is greater than or equal to error level.
func (l *DefaultLogger) Error(args ...interface{}) {
	if l.canLogError() {
		s := "[error] " + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
	}
}

// Fatal logs the given arguments at fatal level if the level is greater than or equal to fatal level.
// It calls os.Exit() after logging.
func (l *DefaultLogger) Fatal(args ...interface{}) {
	if l.canLogFatal() {
		s := "[fatal] " + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
		os.Exit(1)
	}
}

// Panic logs the given arguments at panic level if the level is greater than or equal to panic level.
// It panics after logging.
func (l *DefaultLogger) Panic(args ...interface{}) {
	if l.canLogPanic() {
		s := "[panic] " + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
		panic(s)
	}
}

func (l *DefaultLogger) canLogTrace() bool {
	return l.Level >= TraceLevel
}

func (l *DefaultLogger) canLogInfo() bool {
	return l.Level >= InfoLevel
}

func (l *DefaultLogger) canLogWarn() bool {
	return l.Level >= WarnLevel
}

func (l *DefaultLogger) canLogError() bool {
	return l.Level >= ErrorLevel
}

func (l *DefaultLogger) canLogPanic() bool {
	return l.Level >= PanicLevel
}

func (l *DefaultLogger) canLogFatal() bool {
	return l.Level >= FatalLevel
}

func (l *DefaultLogger) canLogDebug() bool {
	return l.Level >= DebugLevel
}
