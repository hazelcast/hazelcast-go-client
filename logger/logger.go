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

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

// Logger interface is used to provide a custom logger to the client.
// weight of type Weight, describes the log level and f function returns the message to be logged when called.
type Logger interface {
	Log(weight Weight, f func() string)
}

// Weight is the importance of a log message, and used with custom loggers.
// Lower weights are more important than higher weights.
type Weight int

const (
	// WeightOff means: do not log anything.
	WeightOff Weight = 0
	// WeightFatal is for critical errors which halt the client.
	WeightFatal Weight = 100
	// WeightError is for severe errors.
	WeightError Weight = 200
	// WeightWarn is for noting problems.
	// The client can continue running.
	WeightWarn Weight = 300
	// WeightInfo is for informational messages.
	WeightInfo Weight = 400
	// WeightDebug is for logs messages which can help with diagnosing a problem.
	WeightDebug Weight = 500
	// WeightTrace is for potentially very detailed log messages, which are usually logged when an important function is called.
	// Should not be used in production.
	WeightTrace Weight = 600
)

// WeightForLogLevel returns the corresponding logger Weight with the given string if it exists, otherwise returns an error.
func WeightForLogLevel(logLevel Level) (Weight, error) {
	switch logLevel {
	case TraceLevel:
		return WeightTrace, nil
	case DebugLevel:
		return WeightDebug, nil
	case InfoLevel:
		return WeightInfo, nil
	case WarnLevel:
		return WeightWarn, nil
	case ErrorLevel:
		return WeightError, nil
	case FatalLevel:
		return WeightFatal, nil
	case OffLevel:
		return WeightOff, nil
	default:
		return 0, ihzerrors.NewIllegalArgumentError(fmt.Sprintf("no logger level found for %s", logLevel), nil)
	}
}
