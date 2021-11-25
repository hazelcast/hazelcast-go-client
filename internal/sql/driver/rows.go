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

package driver

import (
	"database/sql/driver"
	"io"

	isql "github.com/hazelcast/hazelcast-go-client/internal/sql"
)

type Rows struct {
	row   *isql.Row
	index int
}

func (r *Rows) Columns() []string {
	names := make([]string, len(r.row.Metadata.Columns))
	for i := 0; i < len(names); i++ {
		names[i] = r.row.Metadata.Columns[i].Name
	}
	return names
}

func (r *Rows) Close() error {
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	cols := r.row.Columns
	if r.index >= len(cols) {
		return io.EOF
	}
	for i := 0; i < len(cols); i++ {
		dest[i] = cols[i]
	}
	return nil
}
