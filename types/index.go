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
	IndexTypeSorted IndexType = 0
	IndexTypeHash   IndexType = 1
	IndexTypeBitmap IndexType = 2
)

type UniqueKeyTransformation int32

const (
	UniqueKeyTransformationObject UniqueKeyTransformation = 0
	UniqueKeyTransformationLong   UniqueKeyTransformation = 1
	UniqueKeyTransformationRaw    UniqueKeyTransformation = 2
)

type IndexConfig struct {
	Name               string
	Attributes         []string
	BitmapIndexOptions BitmapIndexOptions
	Type               IndexType
}

type BitmapIndexOptions struct {
	UniqueKey               string
	UniqueKeyTransformation UniqueKeyTransformation
}
