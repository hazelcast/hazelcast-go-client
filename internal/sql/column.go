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

package sql

type ColumnType int32

const (
	ColumnTypeVarchar               ColumnType = 0
	ColumnTypeBoolean               ColumnType = 1
	ColumnTypeTinyInt               ColumnType = 2
	ColumnTypeSmallInt              ColumnType = 3
	ColumnTypeInt                   ColumnType = 4
	ColumnTypeBigInt                ColumnType = 5
	ColumnTypeDecimal               ColumnType = 6
	ColumnTypeReal                  ColumnType = 7
	ColumnTypeDouble                ColumnType = 8
	ColumnTypeDate                  ColumnType = 9
	ColumnTypeTime                  ColumnType = 10
	ColumnTypeTimestamp             ColumnType = 11
	ColumnTypeTimestampWithTimeZone ColumnType = 12
	ColumnTypeObject                ColumnType = 13
	ColumnTypeNull                  ColumnType = 14
	ColumnTypeJSON                  ColumnType = 15
)

type ColumnMetadata struct {
	Name     string
	Type     ColumnType
	Nullable bool
}
