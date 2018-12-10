// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"github.com/hazelcast/hazelcast-go-client/core/logger"
)

type hazelcastLogger struct {
	logger          logger.Logger
	hazelcastPrefix string
}

func newHazelcastLogger(logger logger.Logger, hazelcastPrefix string) *hazelcastLogger {
	return &hazelcastLogger{
		logger:          logger,
		hazelcastPrefix: hazelcastPrefix,
	}
}

func (h *hazelcastLogger) Debug(args ...interface{}) {
	h.logger.Debug(h.hazelcastPrefix, args)
}

func (h *hazelcastLogger) Trace(args ...interface{}) {
	h.logger.Trace(h.hazelcastPrefix, args)
}

func (h *hazelcastLogger) Info(args ...interface{}) {
	h.logger.Info(h.hazelcastPrefix, args)
}

func (h *hazelcastLogger) Warn(args ...interface{}) {
	h.logger.Warn(h.hazelcastPrefix, args)
}

func (h *hazelcastLogger) Error(args ...interface{}) {
	h.logger.Error(h.hazelcastPrefix, args)
}
