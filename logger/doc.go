/*
Package logger contains logging logger.Config structure by which you can specify the log level you want or provide a custom logger. If nothing is provided, default logger with default log level(info) is used.

Log levels are described with type Level and, these correspond to a numeric value that is of Weight type. Levels are:
	- off   -> logger.OffLevel (is used to describe the logger is off)
	- fatal -> logger.FatalLevel
	- error -> logger.ErrorLevel
	- warn  -> logger.WarnLevel
	- info  -> logger.InfoLevel
	- debug -> logger.DebugLevel
	- trace -> logger.TraceLevel
For example, if you want to see logs of level "warn" or more critical ("error", "fatal"), you need to set the log level as warn.
	config := hazelcast.Config{}
	config.Logger.Level = logger.WarnLevel
Setting it to "trace" enables logging for every level.

You can also provide a custom logger that satisfies logger.Logger similarly:
	config := hazelcast.Config{}
	var customLogger logger.Logger := MyCustomLogger{}
	config.Logger.Custom = logger.WarnLevel
See the example for a detailed custom logger implementation.
*/
package logger
