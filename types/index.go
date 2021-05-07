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

package types

type IndexType int32

const (
	IndexTypeSorted = int32(0)
	IndexTypeHash   = int32(1)
	IndexTypeBitmap = int32(2)
)

const (
	UniqueKeyTransformationObject = int32(0)
	UniqueKeyTransformationLong   = int32(1)
	UniqueKeyTransformationRaw    = int32(2)
)

type IndexConfig struct {
	Name               string
	Type               int32
	Attributes         []string
	BitmapIndexOptions BitmapIndexOptions
}

type BitmapIndexOptions struct {
	UniqueKey               string
	UniqueKeyTransformation int32
}
