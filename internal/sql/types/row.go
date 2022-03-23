/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/sql"
)

var ErrIndexOutOfRange = fmt.Errorf("index out of range")
var ErrColumnNotFound = fmt.Errorf("column not found")

// RowMetadata represents SQL row metadata.
type RowMetadata struct {
	NameToIndex map[string]int
	Columns     []sql.ColumnMetadata
}

func (r RowMetadata) GetColumn(index int) (sql.ColumnMetadata, error) {
	if index >= len(r.Columns) || index < 0 {
		return nil, ErrIndexOutOfRange
	}
	return r.Columns[index], nil
}

func (r RowMetadata) FindColumn(columnName string) (int, error) {
	i, ok := r.NameToIndex[columnName]
	if !ok {
		return i, ErrColumnNotFound
	}
	return i, nil
}

func (r RowMetadata) GetColumnCount() int {
	return len(r.Columns)
}

func (r RowMetadata) GetColumns() []sql.ColumnMetadata {
	return r.Columns
}

func NewRowMetadata(columns []sql.ColumnMetadata) RowMetadata {
	var rm RowMetadata
	m := make(map[string]int, len(columns))
	for i, c := range columns {
		m[c.GetName()] = i
	}
	rm.NameToIndex = m
	rm.Columns = columns
	return rm
}
