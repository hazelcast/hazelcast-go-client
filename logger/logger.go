package logger

// Logger is the interface that is used by client for logging.
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
	// CanLogDebug returns true if this logger can log messages in the debug level
	CanLogDebug() bool
}
