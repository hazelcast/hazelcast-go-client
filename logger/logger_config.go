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
)

type Level string

const (
	// OffLevel disables logging.
	OffLevel Level = "off"
	// CriticalLevel level. Used for errors that should definitely be noted.
	CriticalLevel Level = "critical"
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	ErrorLevel Level = "error"
	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel Level = "warn"
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	InfoLevel Level = "info"
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel Level = "debug"
	// TraceLevel level. Designates finer-grained informational events than the Debug.
	TraceLevel Level = "trace"
)

func (l Level) String() string {
	return string(l)
}

type Config struct {
	Level  Level  `json:",omitempty"`
	Custom Logger `json:"-"`
}

func (c *Config) SetLogger(customLogger Logger) {
	c.Custom = customLogger
}

func (c Config) Clone() Config {
	return Config{Level: c.Level, Custom: c.Custom}
}

func (c *Config) Validate() error {
	if c.Level == "" {
		c.Level = InfoLevel
	}
	switch string(c.Level) {
	case "off":
		fallthrough
	case "critical":
		fallthrough
	case "error":
		fallthrough
	case "warn":
		fallthrough
	case "info":
		fallthrough
	case "debug":
		fallthrough
	case "trace":
		return nil
	}
	return fmt.Errorf("invalid logger level: %s", c.Level)
}
