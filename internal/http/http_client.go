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

package http

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/cb"
)

type Header struct {
	Name  string
	Value string
}

func NewHeader(name, value string) Header {
	return Header{Name: name, Value: value}
}

type Client struct {
	httpClient *http.Client
	cb         *cb.CircuitBreaker
}

func NewClient() *Client {
	// TODO: make circuit breaker configurable
	cbr := cb.NewCircuitBreaker(
		cb.MaxRetries(3),
		cb.MaxFailureCount(3),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration(attempt) * time.Second
		}))
	return &Client{
		httpClient: &http.Client{},
		cb:         cbr,
	}
}

func (c *Client) Get(ctx context.Context, url string, headers ...Header) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	for _, h := range headers {
		req.Header.Add(h.Name, h.Value)
	}
	i, err := c.cb.TryContext(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if resp, err := c.httpClient.Do(req); err != nil {
			return nil, err
		} else if resp.StatusCode < 300 {
			return resp, nil
		} else if resp.StatusCode > 500 {
			return nil, NewErrorFromResponse(resp)
		} else {
			return nil, cb.WrapNonRetryableError(NewErrorFromResponse(resp))
		}
	})
	if err != nil {
		return nil, err
	}
	resp := i.(*http.Response)
	b, err := ioutil.ReadAll(resp.Body)
	// error is unhandled
	resp.Body.Close()
	return b, err
}

func (c *Client) GetJSON(ctx context.Context, url string, headers ...Header) (map[string]interface{}, error) {
	r := map[string]interface{}{}
	if b, err := c.Get(ctx, url, headers...); err != nil {
		return nil, err
	} else if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return r, nil
}
