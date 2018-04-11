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
	SQL_PREDICATE = iota
	AND_PREDICATE
	BETWEEN_PREDICATE
	EQUAL_PREDICATE
	GREATERLESS_PREDICATE
	LIKE_PREDICATE
	ILIKE_PREDICATE
	IN_PREDICATE
	INSTANCEOF_PREDICATE
	NOTEQUAL_PREDICATE
	NOT_PREDICATE
	OR_PREDICATE
	REGEX_PREDICATE
	FALSE_PREDICATE
	TRUE_PREDICATE
	PAGING_PREDICATE
	PARTITION_PREDICATE
	NULL_OBJECT
)
