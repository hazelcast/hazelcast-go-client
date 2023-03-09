/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package serialization

import (
	"fmt"

	pserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

type FieldKindOperation interface {
	KindSizeInBytes() int32
}

const variableKindSize = -1

type BooleanFieldKindOperation struct{}

func (BooleanFieldKindOperation) KindSizeInBytes() int32 {
	//Boolean is actually 1 bit. To make it look like smaller than Byte we use 0.
	return 0
}

type ArrayOfBooleanFieldKindOperation struct{}

func (ArrayOfBooleanFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type Int8FieldKindOperation struct{}

func (Int8FieldKindOperation) KindSizeInBytes() int32 {
	return ByteSizeInBytes
}

type ArrayOfInt8FieldKindOperation struct{}

func (ArrayOfInt8FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type Int16FieldKindOperation struct{}

func (Int16FieldKindOperation) KindSizeInBytes() int32 {
	return Int16SizeInBytes
}

type ArrayOfInt16FieldKindOperation struct{}

func (ArrayOfInt16FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type Int32FieldKindOperation struct{}

func (Int32FieldKindOperation) KindSizeInBytes() int32 {
	return Int32SizeInBytes
}

type ArrayOfInt32FieldKindOperation struct{}

func (ArrayOfInt32FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type Int64FieldKindOperation struct{}

func (Int64FieldKindOperation) KindSizeInBytes() int32 {
	return Int64SizeInBytes
}

type ArrayOfInt64FieldKindOperation struct{}

func (ArrayOfInt64FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type Float32FieldKindOperation struct{}

func (Float32FieldKindOperation) KindSizeInBytes() int32 {
	return Float32SizeInBytes
}

type ArrayOfFloat32FieldKindOperation struct{}

func (ArrayOfFloat32FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type Float64FieldKindOperation struct{}

func (Float64FieldKindOperation) KindSizeInBytes() int32 {
	return Float64SizeInBytes
}

type ArrayOfFloat64FieldKindOperation struct{}

func (ArrayOfFloat64FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type StringFieldKindOperation struct{}

func (StringFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfStringFieldKindOperation struct{}

func (ArrayOfStringFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type DecimalFieldKindOperation struct{}

func (DecimalFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfDecimalFieldKindOperation struct{}

func (ArrayOfDecimalFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type TimeFieldKindOperation struct{}

func (TimeFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfTimeFieldKindOperation struct{}

func (ArrayOfTimeFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type DateFieldKindOperation struct{}

func (DateFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfDateFieldKindOperation struct{}

func (ArrayOfDateFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type TimestampFieldKindOperation struct{}

func (TimestampFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfTimestampFieldKindOperation struct{}

func (ArrayOfTimestampFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type TimestampWithTimezoneFieldKindOperation struct{}

func (TimestampWithTimezoneFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfTimestampWithTimezoneFieldKindOperation struct{}

func (ArrayOfTimestampWithTimezoneFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type CompactFieldKindOperation struct{}

func (CompactFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfCompactFieldKindOperation struct{}

func (ArrayOfCompactFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type NullableBooleanFieldKindOperation struct{}

func (NullableBooleanFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfNullableBooleanFieldKindOperation struct{}

func (ArrayOfNullableBooleanFieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type NullableInt8FieldKindOperation struct{}

func (NullableInt8FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfNullableInt8FieldKindOperation struct{}

func (ArrayOfNullableInt8FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type NullableInt16FieldKindOperation struct{}

func (NullableInt16FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfNullableInt16FieldKindOperation struct{}

func (ArrayOfNullableInt16FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type NullableInt32FieldKindOperation struct{}

func (NullableInt32FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfNullableInt32FieldKindOperation struct{}

func (ArrayOfNullableInt32FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type NullableInt64FieldKindOperation struct{}

func (NullableInt64FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfNullableInt64FieldKindOperation struct{}

func (ArrayOfNullableInt64FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type NullableFloat32FieldKindOperation struct{}

func (NullableFloat32FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfNullableFloat32FieldKindOperation struct{}

func (ArrayOfNullableFloat32FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type NullableFloat64FieldKindOperation struct{}

func (NullableFloat64FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

type ArrayOfNullableFloat64FieldKindOperation struct{}

func (ArrayOfNullableFloat64FieldKindOperation) KindSizeInBytes() int32 {
	return variableKindSize
}

func FieldOperations(fieldKind pserialization.FieldKind) FieldKindOperation {
	switch fieldKind {
	case pserialization.FieldKindBoolean:
		return BooleanFieldKindOperation{}
	case pserialization.FieldKindArrayOfBoolean:
		return ArrayOfBooleanFieldKindOperation{}
	case pserialization.FieldKindInt8:
		return Int8FieldKindOperation{}
	case pserialization.FieldKindArrayOfInt8:
		return ArrayOfInt8FieldKindOperation{}
	case pserialization.FieldKindInt16:
		return Int16FieldKindOperation{}
	case pserialization.FieldKindArrayOfInt16:
		return ArrayOfInt16FieldKindOperation{}
	case pserialization.FieldKindInt32:
		return Int32FieldKindOperation{}
	case pserialization.FieldKindArrayOfInt32:
		return ArrayOfInt32FieldKindOperation{}
	case pserialization.FieldKindInt64:
		return Int64FieldKindOperation{}
	case pserialization.FieldKindArrayOfInt64:
		return ArrayOfInt64FieldKindOperation{}
	case pserialization.FieldKindFloat32:
		return Float32FieldKindOperation{}
	case pserialization.FieldKindArrayOfFloat32:
		return ArrayOfFloat32FieldKindOperation{}
	case pserialization.FieldKindFloat64:
		return Float64FieldKindOperation{}
	case pserialization.FieldKindArrayOfFloat64:
		return ArrayOfFloat64FieldKindOperation{}
	case pserialization.FieldKindString:
		return StringFieldKindOperation{}
	case pserialization.FieldKindArrayOfString:
		return ArrayOfStringFieldKindOperation{}
	case pserialization.FieldKindDecimal:
		return DecimalFieldKindOperation{}
	case pserialization.FieldKindArrayOfDecimal:
		return ArrayOfDecimalFieldKindOperation{}
	case pserialization.FieldKindTime:
		return TimeFieldKindOperation{}
	case pserialization.FieldKindArrayOfTime:
		return ArrayOfTimeFieldKindOperation{}
	case pserialization.FieldKindDate:
		return DateFieldKindOperation{}
	case pserialization.FieldKindArrayOfDate:
		return ArrayOfDateFieldKindOperation{}
	case pserialization.FieldKindTimestamp:
		return TimestampFieldKindOperation{}
	case pserialization.FieldKindArrayOfTimestamp:
		return ArrayOfTimestampFieldKindOperation{}
	case pserialization.FieldKindTimestampWithTimezone:
		return TimestampWithTimezoneFieldKindOperation{}
	case pserialization.FieldKindArrayOfTimestampWithTimezone:
		return ArrayOfTimestampWithTimezoneFieldKindOperation{}
	case pserialization.FieldKindCompact:
		return CompactFieldKindOperation{}
	case pserialization.FieldKindArrayOfCompact:
		return ArrayOfCompactFieldKindOperation{}
	case pserialization.FieldKindNullableBoolean:
		return NullableBooleanFieldKindOperation{}
	case pserialization.FieldKindArrayOfNullableBoolean:
		return ArrayOfNullableBooleanFieldKindOperation{}
	case pserialization.FieldKindNullableInt8:
		return NullableInt8FieldKindOperation{}
	case pserialization.FieldKindArrayOfNullableInt8:
		return ArrayOfNullableInt8FieldKindOperation{}
	case pserialization.FieldKindNullableInt16:
		return NullableInt16FieldKindOperation{}
	case pserialization.FieldKindArrayOfNullableInt16:
		return ArrayOfNullableInt16FieldKindOperation{}
	case pserialization.FieldKindNullableInt32:
		return NullableInt32FieldKindOperation{}
	case pserialization.FieldKindArrayOfNullableInt32:
		return ArrayOfNullableInt32FieldKindOperation{}
	case pserialization.FieldKindNullableInt64:
		return NullableInt64FieldKindOperation{}
	case pserialization.FieldKindArrayOfNullableInt64:
		return ArrayOfNullableInt64FieldKindOperation{}
	case pserialization.FieldKindNullableFloat32:
		return NullableFloat32FieldKindOperation{}
	case pserialization.FieldKindArrayOfNullableFloat32:
		return ArrayOfNullableFloat32FieldKindOperation{}
	case pserialization.FieldKindNullableFloat64:
		return NullableFloat64FieldKindOperation{}
	case pserialization.FieldKindArrayOfNullableFloat64:
		return ArrayOfNullableFloat64FieldKindOperation{}
	default:
		panic(fmt.Sprintf("Unknown field kind for field operations: %d", fieldKind))
	}
}
