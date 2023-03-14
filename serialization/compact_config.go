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
	"math/big"
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// CompactConfig contains compact serializers.
type CompactConfig struct {
	serializers map[string]CompactSerializer
}

// Clone creates a copy of the CompactConfig value.
// This method is intended for internal use.
func (cc *CompactConfig) Clone() CompactConfig {
	var clone CompactConfig
	m := make(map[string]CompactSerializer, len(cc.serializers))
	for k, v := range cc.serializers {
		m[k] = v
	}
	clone.serializers = m
	return clone
}

// Serializers returns the registered compact serializers.
// This method is intended for internal use.
func (cc *CompactConfig) Serializers() map[string]CompactSerializer {
	return cc.serializers
}

// Validate validates the CompactConfig and adds default values.
// This method is intended for internal use.
func (cc *CompactConfig) Validate() error {
	cc.ensureSerializers()
	if err := cc.checkNoDefaultSerializer(); err != nil {
		return err
	}
	return nil
}

// SetSerializers sets the compact serializers.
// Each call overrides the previously set serializers.
// It has no effect after hazelcast.Client.StartNewClientWithConfig is called.
func (cc *CompactConfig) SetSerializers(serializers ...CompactSerializer) {
	cc.ensureSerializers()
	for _, ser := range serializers {
		cc.serializers[ser.TypeName()] = ser
	}
}

func (cc *CompactConfig) ensureSerializers() {
	if cc.serializers == nil {
		cc.serializers = make(map[string]CompactSerializer)
	}
}

func (cc *CompactConfig) checkNoDefaultSerializer() error {
	// note that the check we are doing is just information.
	// the default serializers are looked up before the compact serializer,
	// so the user cannot override any default serializer.
	var ds IdentifiedDataSerializable
	var p Portable
	var b *big.Int
	ss := []reflect.Type{
		reflect.TypeOf(ds),
		reflect.TypeOf(p),
		reflect.TypeOf(byte(0)),
		reflect.TypeOf(false),
		reflect.TypeOf(""),
		reflect.TypeOf(uint16(0)),
		reflect.TypeOf(int16(0)),
		reflect.TypeOf(int32(0)),
		reflect.TypeOf(int64(0)),
		reflect.TypeOf(float32(0)),
		reflect.TypeOf(float64(0)),
		reflect.TypeOf([]bool{}),
		reflect.TypeOf([]string{}),
		reflect.TypeOf([]byte{}),
		reflect.TypeOf([]uint16{}),
		reflect.TypeOf([]int16{}),
		reflect.TypeOf([]float32{}),
		reflect.TypeOf([]float64{}),
		reflect.TypeOf(types.UUID{}),
		reflect.TypeOf(b),
		reflect.TypeOf(types.Decimal{}),
		reflect.TypeOf(JSON("")),
		reflect.TypeOf([]interface{}{}),
		reflect.TypeOf(types.LocalDate{}),
		reflect.TypeOf(types.LocalTime{}),
		reflect.TypeOf(types.LocalDateTime{}),
		reflect.TypeOf(types.OffsetDateTime{}),
	}
	ssm := map[reflect.Type]struct{}{}
	for _, s := range ss {
		ssm[s] = struct{}{}
	}
	for _, s := range cc.serializers {
		if _, ok := ssm[s.Type()]; ok {
			return fmt.Errorf("registering serializer for %s: overriding the default serializer is not allowed: %w", s.TypeName(), hzerrors.ErrIllegalArgument)
		}
	}
	return nil
}
