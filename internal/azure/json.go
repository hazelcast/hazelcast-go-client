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

package azure

import (
	"context"

	"github.com/hazelcast/hazelcast-go-client/internal/http"
)

func jsonStringValue(m map[string]interface{}, key string) string {
	if i, ok := m[key]; ok {
		if s, ok := i.(string); ok {
			return s
		}
	}
	return ""
}

func jsonString(j interface{}) string {
	if js, ok := j.(string); ok {
		return js
	}
	return ""
}

func jsonObject(j interface{}) map[string]interface{} {
	if t, ok := j.(map[string]interface{}); ok {
		return t
	}
	return nil

}

func jsonArray(j interface{}) []interface{} {
	if t, ok := j.([]interface{}); ok {
		return t
	}
	return nil
}

func jsonStringObject(j interface{}) map[string]string {
	if t, ok := j.(map[string]interface{}); ok {
		m := map[string]string{}
		for k, v := range t {
			if vs, ok := v.(string); ok {
				m[k] = vs
			}
		}
		return m
	}
	return nil
}

func jsonObjectGet(j interface{}, key string) interface{} {
	if jm, ok := j.(map[string]interface{}); ok {
		return jm[key]
	}
	return nil
}

func getJSON(ctx context.Context, h *http.Client, url string) (map[string]interface{}, error) {
	return h.GetJSON(ctx, url, http.NewHeader("Metadata", "true"))
}
