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
	"runtime"
)

const (
	// logCallDepth is used for removing the last two method names from call trace when logging file names.
	logCallDepth    = 2
	defaultLogLevel = infoLevel
)

// DefaultLogger has Go's built in log embedded in it. It adds level logging.
type DefaultLogger struct {
	*log.Logger
	Level          int
	versionMessage string
}

// New returns a Default Logger with defaultLogLevel.
func New() *DefaultLogger {
	return &DefaultLogger{
		Logger: log.New(os.Stderr, "", log.LstdFlags),
		Level:  defaultLogLevel,
	}
}

// NewWithVersion returns a Default Logger with versionMessage set as a combination of
// groupName clientName and clientVersion to be used in logging.
func NewWithVersion(groupName string, clientName string, clientVersion string) *DefaultLogger {
	versionMessage := clientName + " [" + groupName + "]" + " [" + clientVersion + "] "
	return &DefaultLogger{
		Logger:         log.New(os.Stderr, "", log.LstdFlags),
		Level:          defaultLogLevel,
		versionMessage: versionMessage,
	}
}

// Debug logs the given arguments at debug level if the level is greater than or equal to debug level.
func (l *DefaultLogger) Debug(args ...interface{}) {
	if l.canLogDebug() {
		callerName := l.findCallerFuncName()
		s := callerName + "\n" + "DEBUG: " + " " + l.versionMessage + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
	}
}

// Trace logs the given arguments at trace level if the level is greater than or equal to trace level.
func (l *DefaultLogger) Trace(args ...interface{}) {
	if l.canLogTrace() {
		callerName := l.findCallerFuncName()
		s := callerName + "\n" + "TRACE: " + " " + l.versionMessage + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
	}
}

// Info logs the given arguments at info level if the level is greater than or equal to info level.
func (l *DefaultLogger) Info(args ...interface{}) {
	if l.canLogInfo() {
		callerName := l.findCallerFuncName()
		s := callerName + "\n" + "INFO: " + " " + l.versionMessage + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
	}
}

// Warn logs the given arguments at warn level if the level is greater than or equal to warn level.
func (l *DefaultLogger) Warn(args ...interface{}) {
	if l.canLogWarn() {
		callerName := l.findCallerFuncName()
		s := callerName + "\n" + "WARN: " + " " + l.versionMessage + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
	}
}

// Error logs the given arguments at error level if the level is greater than or equal to error level.
func (l *DefaultLogger) Error(args ...interface{}) {
	if l.canLogError() {
		callerName := l.findCallerFuncName()
		s := callerName + "\n" + "ERROR: " + " " + l.versionMessage + fmt.Sprintln(args...)
		l.Output(logCallDepth, s)
	}
}

func (l *DefaultLogger) findCallerFuncName() string {
	pc, _, _, _ := runtime.Caller(2)
	return runtime.FuncForPC(pc).Name()
}

func (l *DefaultLogger) canLogTrace() bool {
	return l.Level >= traceLevel
}

func (l *DefaultLogger) canLogInfo() bool {
	return l.Level >= infoLevel
}

func (l *DefaultLogger) canLogWarn() bool {
	return l.Level >= warnLevel
}

func (l *DefaultLogger) canLogError() bool {
	return l.Level >= errorLevel
}

func (l *DefaultLogger) canLogDebug() bool {
	return l.Level >= debugLevel
}
