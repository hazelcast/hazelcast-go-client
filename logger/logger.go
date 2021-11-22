package logger

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

// Logger is the interface that is used by client for logging.
// Level is: type Level string
type Logger interface {
	Log(weight Weight, f func() string)
}

type Weight int

const (
	WeightOff Weight = iota * 100
	WeightFatal
	WeightError
	WeightWarn
	WeightInfo
	WeightDebug
	WeightTrace
)

// GetLogLevel returns the corresponding logger Weight with the given string if it exists, otherwise returns an error.
func GetLogLevel(logLevel Level) (Weight, error) {
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
		return 0, hzerrors.NewIllegalArgumentError(fmt.Sprintf("no logger level found for %s", logLevel), nil)
	}
}
