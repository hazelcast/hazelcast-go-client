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

package cluster

import "errors"

var ErrConfigInvalidClusterName = errors.New("invalid cluster name")
var ErrConfigInvalidConnectionTimeout = errors.New("invalid connection timeout")
var ErrConfigInvalidHeartbeatInterval = errors.New("invalid heartbeat interval")
var ErrConfigInvalidHeartbeatTimeout = errors.New("invalid heartbeat timeout")
var ErrConfigInvalidInvocationTimeout = errors.New("invalid invocation timeout")
