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

package nearcache

type MaxSizePolicy int32

const (
	MaxSizePolicyPerNode                MaxSizePolicy = 0
	MaxSizePolicyPerPartition           MaxSizePolicy = 1
	MaxSizePolicyUsedHeapPercentage     MaxSizePolicy = 2
	MaxSizePolicyUsedHeapSize           MaxSizePolicy = 3
	MaxSizePolicyHeapPercentage         MaxSizePolicy = 4
	MaxSizePolicyHeapSize               MaxSizePolicy = 5
	MaxSizePolicyEntryCount             MaxSizePolicy = 6
	MaxSizePolicyUsedNativeMemorySize   MaxSizePolicy = 7
	MaxSizePolicyNativeMemoryPercentage MaxSizePolicy = 8
	MaxSizePolicyNativeMemorySize       MaxSizePolicy = 9
	MaxSizePolicyFreeMemoryPercentage   MaxSizePolicy = 10
)
