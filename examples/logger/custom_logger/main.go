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

package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

type CustomLogger struct {
	loggers []*log.Logger
	weight  logger.Weight
}

func NewCustomLogger(level logger.Level) CustomLogger {
	weight, err := logger.WeightForLogLevel(level)
	if err != nil {
		panic(err)
	}
	return CustomLogger{
		weight: weight,
		loggers: []*log.Logger{
			nil, // OFF
			log.New(os.Stderr, "[FATAL] ", log.LstdFlags),
			log.New(os.Stderr, "[ERROR] ", log.LstdFlags),
			log.New(os.Stderr, "[WARN ] ", log.LstdFlags),
			log.New(os.Stderr, "[INFO ] ", log.LstdFlags),
			log.New(os.Stderr, "[DEBUG] ", log.LstdFlags|log.Lshortfile),
			log.New(os.Stderr, "[TRACE] ", log.LstdFlags|log.Llongfile),
		},
	}
}

func (c CustomLogger) Log(wantWeight logger.Weight, formatter func() string) {
	// Do not log if this is a more detailed log message than configured.
	if c.weight < wantWeight {
		return
	}
	// Get the logger.
	idx := wantWeight / 100
	// wantWeight is never 0 (OFF), so the following commented out code is not necessary
	//if idx == 0 {
	//	return
	//}
	lg := c.loggers[idx]
	// Call formatter and log the resulting string.
	// Ignoring the error here.
	_ = lg.Output(3, formatter())
}

func main() {
	// Create the default configuration.
	config := hazelcast.Config{}
	// Set the custom logger.
	config.Logger.CustomLogger = NewCustomLogger(logger.TraceLevel)
	// Start the client.
	client, err := hazelcast.StartNewClientWithConfig(context.TODO(), config)
	if err != nil {
		panic(err)
	}
	// Wait around a bit to log some messages.
	time.Sleep(5 * time.Second)
	// Time to release client resources and exit.
	if err := client.Shutdown(context.TODO()); err != nil {
		panic(err)
	}
}
