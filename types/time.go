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

import (
	"time"
)

// LocalDate is the date part of time.Time.
type LocalDate time.Time

// LocalTime is the time part of time.Time.
type LocalTime time.Time

// LocalDateTime is the date and time with local timezone.
type LocalDateTime time.Time

// OffsetDateTime is the date and time with a timezone.
type OffsetDateTime time.Time

func (ld *LocalDate) ToTime() time.Time {
	if ld == nil {
		return time.Time{}
	}
	return *(*time.Time)(ld)
}

func (ld *LocalDate) String() string {
	if ld == nil {
		return ""
	}
	return (*time.Time)(ld).String()
}

func (lt *LocalTime) ToTime() time.Time {
	if lt == nil {
		return time.Time{}
	}
	return *(*time.Time)(lt)
}

func (lt *LocalTime) String() string {
	if lt == nil {
		return ""
	}
	return (*time.Time)(lt).String()
}

func (ldt *LocalDateTime) ToTime() time.Time {
	if ldt == nil {
		return time.Time{}
	}
	return *(*time.Time)(ldt)
}

func (ldt *LocalDateTime) String() string {
	if ldt == nil {
		return ""
	}
	return (*time.Time)(ldt).String()
}

func (odt *OffsetDateTime) ToTime() time.Time {
	if odt == nil {
		return time.Time{}
	}
	return *(*time.Time)(odt)
}

func (odt *OffsetDateTime) String() string {
	if odt == nil {
		return ""
	}
	return (*time.Time)(odt).String()
}
