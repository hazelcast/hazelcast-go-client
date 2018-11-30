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

package logger

// NoLogger does not log anything.
type NoLogger struct {
}

func NewNoLogger() *NoLogger {
	return &NoLogger{}
}

func (*NoLogger) Debug(args ...interface{}) {
}

func (*NoLogger) Trace(args ...interface{}) {
}

func (*NoLogger) Info(args ...interface{}) {
}

func (*NoLogger) Warn(args ...interface{}) {
}

func (*NoLogger) Error(args ...interface{}) {
}
