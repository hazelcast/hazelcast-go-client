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

package predicates

const (
	SQL_PREDICATE         = 0
	AND_PREDICATE         = 1
	BETWEEN_PREDICATE     = 2
	EQUAL_PREDICATE       = 3
	GREATERLESS_PREDICATE = 4
	LIKE_PREDICATE        = 5
	ILIKE_PREDICATE       = 6
	IN_PREDICATE          = 7
	INSTANCEOF_PREDICATE  = 8
	NOTEQUAL_PREDICATE    = 9
	NOT_PREDICATE         = 10
	OR_PREDICATE          = 11
	REGEX_PREDICATE       = 12
	FALSE_PREDICATE       = 13
	TRUE_PREDICATE        = 14
	PAGING_PREDICATE      = 15
	PARTITION_PREDICATE   = 16
	NULL_OBJECT           = 17
)
