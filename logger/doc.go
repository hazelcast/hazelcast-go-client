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

/*
Package logger contains logging related API.

It is possible to use a custom logger, but the builtin logger is used if one is not provided.

Log levels for the builtin logger are described with type Level. Following log levels are supported:

	- OffLevel   : Do not log anything.
	- FatalLevel : A critical error occured, the client cannot continue running.
	- ErrorLevel : A severe error occured.
	- WarnLevel  : There is a problem, but the client can continue running.
	- InfoLevel  : Informational message.
	- DebugLevel : A log message which can help with diagnosing a problem. Should not be used in production.
	- TraceLevel : Potentially very detailed log message, which is usually logged when an important function is called. Should not be used in production.

Lower levels imply higher levels.
For example, if you want to see logs of level WarnLevel or more critical (ErrorLevel, FatalLevel), you need to set the log level as WarnLevel.

	config := hazelcast.Config{}
	config.Logger.Level = logger.WarnLevel

Using a Custom Logger

You can provide a custom logger that implements logger.Logger:

	type Logger interface {
		Log(weight Weight, f func() string)
	}

weight is the numeric value that correspond to the log level.
E.g., WeightInfo corresponds to InfoLevel.
f is a function which produces and returns the log message.
If it is not called, there is no performance penalty related to log production.

The Log method is called whenever the client needs to log.
It's the custom logger's responsibility to decide whether to call f and log a message or avoid calling it to discard the message.
Most custom loggers would have a log filtering logic in their Log methods similar to the one below:

	func (c MyCustomLogger) Log(wantWeight logger.Weight, formatter func() string) {
		// Do not log if this is a more detailed log message than configured.
		if c.weight < wantWeight {
			return
		}
		// ...
	}

In order to activate your custom logger, use config.Logger.CustomLogger configuration:

	config := hazelcast.Config{}
	config.Logger.CustomLogger = MyCustomLogger{}

See the example for a detailed custom logger implementation.
*/
package logger
