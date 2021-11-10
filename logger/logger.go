package logger

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

// Logger is the interface that is used by client for logging.
// Level is: type Level string
type Logger interface {
	Log(level Level, formatter func() string)
}

const (
	offLevel = iota * 100
	criticalLevel
	errorLevel
	warnLevel
	infoLevel
	debugLevel
	traceLevel
)

// GetLogLevel returns the corresponding logger level with the given string if it exists, otherwise returns an error.
func GetLogLevel(logLevel Level) (int, error) {
	switch logLevel {
	case OffLevel:
		return offLevel, nil
	case CriticalLevel:
		return criticalLevel, nil
	case ErrorLevel:
		return errorLevel, nil
	case WarnLevel:
		return warnLevel, nil
	case InfoLevel:
		return infoLevel, nil
	case DebugLevel:
		return debugLevel, nil
	case TraceLevel:
		return traceLevel, nil
	default:
		return 0, hzerrors.NewIllegalArgumentError(fmt.Sprintf("no logger level found for %s", logLevel), nil)
	}
}
