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

package types_test

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/stretchr/testify/assert"
)

func TestNewUUID(t *testing.T) {
	m := make(map[string]bool)
	for x := 1; x < 32; x++ {
		uuid := types.NewUUID()
		s := uuid.String()
		if m[s] {
			t.Errorf("Duplicate UUID: %s", s)
		}
		m[s] = true
	}
}

func TestNewUUIDWith(t *testing.T) {
	uuid1 := types.NewUUID()
	uuid2 := types.NewUUIDWith(uuid1.MostSignificantBits(), uuid1.LeastSignificantBits())
	assert.Equal(t, uuid2, uuid1)
}

func TestUUID_String(t *testing.T) {
	uuid := types.NewUUIDWith(42, 42)
	assert.Equal(t, "00000000-0000-002a-0000-00000000002a", uuid.String())
}

func TestUUID_String_NilUUID(t *testing.T) {
	assert.Equal(t, "00000000-0000-0000-0000-000000000000", types.UUID{}.String())
	assert.True(t, types.UUID{}.Default())
}

func TestUUID_ExtractInto(t *testing.T) {
	uuid := types.NewUUIDWith(0xFA_FB_FC_FD_FE_FF_AA_BB, 0xAB_CD_EF_01_23_45_67_89)
	b := make([]byte, 16)
	uuid.ExtractInto(b)
	target := []byte{0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF, 0xAA, 0xBB, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89}
	assert.Equal(t, target, b)
}

func TestUUID_ExtractInto_SmallArray(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Fatalf("should have failed")
		}
	}()
	uuid := types.NewUUIDWith(0xFA_FB_FC_FD_FE_FF_AA_BB, 0xAB_CD_EF_01_23_45_67_89)
	b := make([]byte, 15)
	uuid.ExtractInto(b)
}
