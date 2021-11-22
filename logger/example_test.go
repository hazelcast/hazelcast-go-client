package logger_test

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

type CustomLogger struct{}

func (cl *CustomLogger) Log(weight logger.Weight, f func() string) {
	var logLevel logger.Level
	switch weight {
	case logger.WeightTrace:
		logLevel = logger.TraceLevel
	case logger.WeightDebug:
		logLevel = logger.DebugLevel
	case logger.WeightInfo:
		logLevel = logger.InfoLevel
	case logger.WeightWarn:
		logLevel = logger.WarnLevel
	case logger.WeightError:
		logLevel = logger.ErrorLevel
	case logger.WeightFatal:
		logLevel = logger.FatalLevel
	case logger.WeightOff:
		logLevel = logger.OffLevel
	default:
		return // unknown level, do not log anything.
	}

	s := fmt.Sprintf("[CustomApp %s]: %s", strings.ToUpper(logLevel.String()), f())
	log.Println(s)
}

func Example() {
	// Create the configuration.
	config := hazelcast.Config{}
	// Set attributes as fit.
	config.Logger.Custom = &CustomLogger{}
	// Start the client with the configuration provider.
	ctx := context.TODO()
	_, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	// Use created client on your application with custom logger.
}
