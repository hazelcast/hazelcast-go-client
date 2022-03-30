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

package sql

// Result represents a query result.
// Depending on the statement type it represents a stream of rows or an update count.
type Result interface {
	// RowMetadata returns metadata information about rows.
	// An error is returned if result represents an update count.
	RowMetadata() (RowMetadata, error)
	// IsRowSet returns whether this result has rows to iterate using the HasNext method.
	IsRowSet() bool
	// UpdateCount returns the number of rows updated by the statement or -1 if this result is a row set.
	// In case the result doesn't contain rows but the update count isn't applicable or known, 0 is returned.
	UpdateCount() int64
	// Iterator returns the RowsIterator over the result rows.
	// The iterator may be requested only once.
	// An error is returned if the iterator is requested more than once, or if the result contains only update count.
	Iterator() (RowsIterator, error)
	// Close notifies the member to release resources for the corresponding query for results that represents a stream of rows.
	// It can be safely called more than once, and it is concurrency-safe.
	// If result represents an update count, it has no effect.
	Close() error
}

type RowsIterator interface {
	// HasNext prepares the next result row for reading via Next method. It
	// returns true on success, or false if there is no next result row or an error
	// happened while preparing it. Err should be consulted to distinguish between
	// the two cases.
	//
	// Every call to Next, even the first one, must be preceded by a call to HasNext.
	HasNext() bool
	// Next returns the current row.
	// Every call to Next, even the first one, must be preceded by a call to HasNext.
	Next() (Row, error)
}

// Row represents an SQL result row.
type Row interface {
	// Get returns the value of the column by index. If index is out of range, an error is returned.
	Get(index int) (interface{}, error)
	// GetByColumnName returns the value of the column by name. If columns does not exist, an error is returned.
	GetByColumnName(name string) (interface{}, error)
	// GetMetadata returns the metadata information about the row.
	GetMetadata() RowMetadata
}
