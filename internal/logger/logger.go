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
	"strings"

	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	publogger "github.com/hazelcast/hazelcast-go-client/logger"
)

const (
	offLevel = iota * 100
	errorLevel
	warnLevel
	infoLevel
	debugLevel
	traceLevel
)

// nameToLevel is used to get corresponding level for log level strings.
var nameToLevel = map[publogger.Level]int{
	publogger.ErrorLevel: errorLevel,
	publogger.WarnLevel:  warnLevel,
	publogger.InfoLevel:  infoLevel,
	publogger.DebugLevel: debugLevel,
	publogger.TraceLevel: traceLevel,
	publogger.OffLevel:   offLevel,
}

// isValidLogLevel returns true if the given log level is valid.
// The check is done case insensitive.
func isValidLogLevel(logLevel publogger.Level) bool {
	logLevelStr := strings.ToLower(string(logLevel))
	_, found := nameToLevel[publogger.Level(logLevelStr)]
	return found
}

// GetLogLevel returns the corresponding log level with the given string if it exists, otherwise returns an error.
func GetLogLevel(logLevel publogger.Level) (int, error) {
	if !isValidLogLevel(logLevel) {
		return 0, hzerrors.NewIllegalArgumentError(fmt.Sprintf("no log level found for %s", logLevel), nil)
	}
	return nameToLevel[logLevel], nil
}
