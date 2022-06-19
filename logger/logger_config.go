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
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

// Level is the importance of a log message.
// It is used by the builtin logger.
// It can also be used by custom loggers.
type Level string

const (
	// OffLevel disables logging.
	OffLevel Level = "off"
	// FatalLevel is for critical errors which halt the client.
	FatalLevel Level = "fatal"
	// ErrorLevel is for severe errors.
	ErrorLevel Level = "error"
	// WarnLevel is for noting problems.
	// The client can continue running.
	WarnLevel Level = "warn"
	// InfoLevel is for informational messages.
	InfoLevel Level = "info"
	// DebugLevel is for logs messages which can help with diagnosing a problem.
	// Should not be used in production.
	DebugLevel Level = "debug"
	// TraceLevel is for potentially very detailed log messages, which are usually logged when an important function is called.
	// Should not be used in production.
	TraceLevel Level = "trace"
)

// String converts a Level to a string.
func (l Level) String() string {
	return string(l)
}

// Config is the logging configuration.
// Using a CustomLogger and specifying a Level is not allowed.
type Config struct {
	// CustomLogger is used to set a custom logger.
	// The configuration in this section does not apply to the custom logger.
	// The custom logger should handle its own log filtering.
	CustomLogger Logger `json:"-"`
	// Level is the log level for the builtin logger.
	Level Level `json:",omitempty"`
}

// Clone returns a copy of the logger configuration.
func (c Config) Clone() Config {
	return Config{Level: c.Level, CustomLogger: c.CustomLogger}
}

// Validate checks the logger configuration for problems and updates it with default values.
func (c *Config) Validate() error {
	if c.Level != "" && c.CustomLogger != nil {
		return ihzerrors.NewIllegalArgumentError("logger level cannot be set when a custom logger is specified", nil)
	}
	if c.Level == "" {
		c.Level = InfoLevel
	}
	if _, err := WeightForLogLevel(c.Level); err != nil {
		return err
	}
	return nil
}
