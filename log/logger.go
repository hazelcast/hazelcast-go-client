package log

import (
	"fmt"
	"strings"

	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

// Logger is the interface that is used by client for logging.
// Level is: type Level string
type Logger interface {
	Log(level Level, formatter func() string)
}

const (
	offLevel = iota * 100
	errorLevel
	warnLevel
	infoLevel
	debugLevel
	traceLevel
)

// nameToLevel is used to get corresponding level for log level strings.
var nameToLevel = map[Level]int{
	ErrorLevel: errorLevel,
	WarnLevel:  warnLevel,
	InfoLevel:  infoLevel,
	DebugLevel: debugLevel,
	TraceLevel: traceLevel,
	OffLevel:   offLevel,
}

// isValidLogLevel returns true if the given log level is valid.
// The check is done case insensitive.
func isValidLogLevel(logLevel Level) bool {
	logLevelStr := strings.ToLower(string(logLevel))
	_, found := nameToLevel[Level(logLevelStr)]
	return found
}

// GetLogLevel returns the corresponding log level with the given string if it exists, otherwise returns an error.
func GetLogLevel(logLevel Level) (int, error) {
	if !isValidLogLevel(logLevel) {
		return 0, hzerrors.NewIllegalArgumentError(fmt.Sprintf("no log level found for %s", logLevel), nil)
	}
	return nameToLevel[logLevel], nil
}
