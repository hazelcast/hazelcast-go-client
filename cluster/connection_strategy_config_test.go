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

package cluster_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
)

func TestReconnectMode_MarshalText(t *testing.T) {
	t.Logf("roconnection-mode on")
	rmOn := cluster.ReconnectModeOn
	text, err := rmOn.MarshalText()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, text, []byte(`on`))
	t.Logf("roconnection-mode off")
	rmOff := cluster.ReconnectModeOff
	text, err = rmOff.MarshalText()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, text, []byte(`off`))
	t.Logf("roconnection-mode invalid")
	rmInvalid := cluster.ReconnectModeOff + 1
	_, err = rmInvalid.MarshalText()
	errors.Is(err, hzerrors.ErrIllegalState)
}

func TestReconnectMode_UnmarshalText(t *testing.T) {
	rm := cluster.ReconnectMode(0)
	t.Logf("roconnection-mode on")
	if err := rm.UnmarshalText([]byte(`on`)); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, rm, cluster.ReconnectModeOn)
	t.Logf("roconnection-mode off")
	if err := rm.UnmarshalText([]byte(`off`)); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, rm, cluster.ReconnectModeOff)
	t.Logf("roconnection-mode invalid")
	err := rm.UnmarshalText([]byte(`invalid`))
	if !errors.Is(err, hzerrors.ErrIllegalState) {
		t.Fatal("error types does not match")
	}
}
