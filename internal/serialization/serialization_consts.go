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

package serialization

const (
	TypeNil               = 0
	TypePortable          = -1
	TypeDataSerializable  = -2
	TypeByte              = -3
	TypeBool              = -4
	TypeUInteger16        = -5
	TypeInteger16         = -6
	TypeInteger32         = -7
	TypeInteger64         = -8
	TypeFloat32           = -9
	TypeFloat64           = -10
	TypeString            = -11
	TypeByteArray         = -12
	TypeBoolArray         = -13
	TypeUInteger16Array   = -14
	TypeInteger16Array    = -15
	TypeInteger32Array    = -16
	TypeInteger64Array    = -17
	TypeFloat32Array      = -18
	TypeFloat64Array      = -19
	TypeStringArray       = -20
	TypeUUID              = -21
	TypeJavaClass         = -24
	TypeJavaArrayList     = -29
	TypeJavaLinkedList    = -30
	TypeJSONSerialization = -130
	TypeGobSerialization  = -140
)
