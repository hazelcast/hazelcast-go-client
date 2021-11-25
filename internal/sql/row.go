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

type Row struct {
	Metadata RowMetadata
	Columns  [][]interface{}
}

func (r Row) ColumnByName(name string) interface{} {
	panic("not implemented")
}

func (r Row) ColumnByIndex(index int) interface{} {
	panic("not implemented")
}

type RowMetadata struct {
	Columns []ColumnMetadata
}

func (m RowMetadata) ColumnByName(name string) ColumnMetadata {
	panic("not implemented")
}
