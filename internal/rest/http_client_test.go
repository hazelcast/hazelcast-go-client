package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// roundTripFunc to mock and assert server side.
type roundTripFunc func(req *http.Request) *http.Response

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

//NewTestClient returns *http.Client with Transport replaced to avoid making real calls
func setTransport(fn roundTripFunc, client *HTTPClient) {
	client.httpClient.Transport = fn
}

func TestHTTPClient_Get(t *testing.T) {
	type args struct {
		serverHandler roundTripFunc
		url           string
		headers       []HTTPHeader
	}
	tests := []struct {
		err  *Error
		args args
		name string
		want []byte
	}{
		{
			name: "happy path, return success and response",
			args: args{
				url: "localhost:8080/some/path",
				headers: []HTTPHeader{
					{
						Name:  "Some-Header",
						Value: "value",
					},
				},
				serverHandler: func(req *http.Request) *http.Response {
					header := req.Header.Get("Some-Header")
					assert.Equal(t, "value", header)
					assert.Equal(t, "localhost:8080/some/path", req.URL.String())
					return &http.Response{
						StatusCode: 200,
						// Send response to be tested
						Body: ioutil.NopCloser(bytes.NewBufferString("OK")),
						// Must be set to non-nil value or it panics
						Header: make(http.Header),
					}
				},
			},
		},
		{
			name: "return error from server",
			args: args{
				serverHandler: func(req *http.Request) *http.Response {
					return &http.Response{
						StatusCode: 500,
						// Send response to be tested
						Body: ioutil.NopCloser(bytes.NewBufferString("FAIL")),
						// Must be set to non-nil value or it panics
						Header: make(http.Header),
					}
				},
			},
			err: NewError(500, "FAIL"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewHTTPClient()
			setTransport(tt.args.serverHandler, c)
			ctx := context.Background()
			resp, err := c.Get(ctx, tt.args.url, tt.args.headers...)
			if tt.err != nil {
				assert.Equal(t, tt.err, err)
				return
			}
			assert.Nil(t, err)
			assert.Equal(t, "OK", string(resp))
		})
	}
}

// type for json tests
type employee struct {
	Name string `json:"name"`
	Job  string `json:"job"`
}

func TestHTTPClient_GetJSONObject(t *testing.T) {
	tmp := employee{
		Name: "Joe",
		Job:  "accountant",
	}
	testResp, err := json.Marshal(tmp)
	assert.Nil(t, err)
	// create corresponding map to assert
	tmpMap := make(map[string]interface{})
	tmpMap["name"] = "Joe"
	tmpMap["job"] = "accountant"
	tests := []struct {
		want          interface{}
		err           error
		serverHandler roundTripFunc
		name          string
	}{
		{
			name: "happy path, return success and response",
			serverHandler: func(req *http.Request) *http.Response {
				return &http.Response{
					StatusCode: 200,
					// Send response to be tested
					Body: ioutil.NopCloser(bytes.NewReader(testResp)),
					// Must be set to non-nil value or it panics
					Header: make(http.Header),
				}
			},
			want: tmpMap,
		},
		{
			name: "return invalid json",
			serverHandler: func(req *http.Request) *http.Response {
				return &http.Response{
					StatusCode: 200,
					// Send response to be tested
					Body: ioutil.NopCloser(bytes.NewReader(testResp[:len(testResp)-4])),
					// Must be set to non-nil value or it panics
					Header: make(http.Header),
				}
			},
			err: &json.SyntaxError{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewHTTPClient()
			setTransport(tt.serverHandler, c)
			ctx := context.Background()
			resp, err := c.GetJSONObject(ctx, "localhost:8080")
			if tt.err != nil {
				assert.IsType(t, tt.err, err)
				return
			}
			assert.Nil(t, err)
			assert.Equal(t, tt.want, resp)
		})
	}
}
