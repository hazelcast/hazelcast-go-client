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

package driver_test

import (
	"database/sql/driver"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	idriver "github.com/hazelcast/hazelcast-go-client/internal/sql/driver"
)

func TestArgNotNil(t *testing.T) {
	var b *big.Int
	nv := &driver.NamedValue{
		Name:    "",
		Ordinal: 0,
		Value:   b,
	}
	conn := idriver.Conn{}
	err := conn.CheckNamedValue(nv)
	assert.Error(t, err)
	assert.Equal(t, "nil arg is not allowed: illegal argument error", err.Error())
}
