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

package cluster

import (
	"fmt"
	"strconv"
	"strings"
)

type portRange struct {
	Begin int
	End   int
}

func newPortRange() portRange {
	return portRange{
		Begin: 5701,
		End:   5703,
	}
}

func (p *portRange) Parse(s string) error {
	if s == "" {
		// return default port range
		return nil
	}
	if !strings.Contains(s, "-") {
		if port, err := strconv.Atoi(s); err != nil {
			return fmt.Errorf("error parsing port range: %w", err)
		} else {
			p.Begin = port
			p.End = port
			return nil
		}
	}
	var err error
	parts := strings.SplitN(s, "-", 2)
	if parts[0] != "" {
		if p.Begin, err = strconv.Atoi(parts[0]); err != nil {
			return fmt.Errorf("error parsing port range: %w", err)
		}
	}
	if parts[1] != "" {
		if p.End, err = strconv.Atoi(parts[1]); err != nil {
			return fmt.Errorf("error parsing port range: %w", err)
		}
	}
	return nil
}
