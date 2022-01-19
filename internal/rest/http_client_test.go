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
type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// setTransport modifies *http.Client by replacing Transport to avoid making real calls
func setTransport(fn roundTripFunc, client *HTTPClient) {
	client.httpClient.Transport = fn
}

func TestHTTPClient_Get(t *testing.T) {
	type args struct {
		serverHandler roundTripFunc
		url           string
		headers       []HTTPHeader
	}
	tcs := []struct {
		err  *Error
		args args
		name string
	}{
		{
			name: "happy path, return success and response",
			args: args{
				url: "localhost:8080/some/path",
				headers: []HTTPHeader{
					NewHTTPHeader("Some-Header", "value"),
				},
				serverHandler: func(req *http.Request) (*http.Response, error) {
					header := req.Header.Get("Some-Header")
					assert.Equal(t, "value", header)
					assert.Equal(t, "localhost:8080/some/path", req.URL.String())
					return &http.Response{
						StatusCode: http.StatusOK,
						// Send response to be tested
						Body: ioutil.NopCloser(bytes.NewBufferString("OK")),
						// Must be set to non-nil value or it panics
						Header: make(http.Header),
					}, nil
				},
			},
		},
		{
			name: "return error from server, status code <= 500",
			args: args{
				serverHandler: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusInternalServerError,
						// Send response to be tested
						Body: ioutil.NopCloser(bytes.NewBufferString("FAIL")),
						// Must be set to non-nil value or it panics
						Header: make(http.Header),
					}, nil
				},
			},
			err: NewError(500, "FAIL"),
		},
	}
	for _, tt := range tcs {
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

func TestHttpClient_Retry(t *testing.T) {
	c := NewHTTPClient()
	ctx := context.Background()
	retry := 1
	setTransport(func(req *http.Request) (*http.Response, error) {
		resp := &http.Response{
			StatusCode: http.StatusOK,
			// Send response to be tested
			Body: ioutil.NopCloser(bytes.NewBufferString("OK")),
			// Must be set to non-nil value or it panics
			Header: make(http.Header),
		}
		if retry < 3 {
			// retry range
			resp.StatusCode = 500
			retry++
		}
		return resp, nil
	}, c)
	resp, err := c.Get(ctx, "somehost:8080")
	assert.Nil(t, err)
	assert.Equal(t, "OK", string(resp))
	assert.Equal(t, 3, retry)
	// test non-retryable status code
	retry = 0
	setTransport(func(req *http.Request) (*http.Response, error) {
		resp := &http.Response{
			StatusCode: http.StatusBadRequest,
			Body:       ioutil.NopCloser(bytes.NewBufferString("OK")),
			Header:     make(http.Header),
		}
		retry++
		return resp, nil
	}, c)
	resp, err = c.Get(ctx, "somehost:8080")
	assert.NotNil(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, 1, retry)
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
	tcs := []struct {
		want          interface{}
		err           error
		serverHandler roundTripFunc
		name          string
	}{
		{
			name: "happy path, return success and response",
			serverHandler: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					// Send response to be tested
					Body: ioutil.NopCloser(bytes.NewReader(testResp)),
					// Must be set to non-nil value or it panics
					Header: make(http.Header),
				}, nil
			},
			want: tmpMap,
		},
		{
			name: "return invalid json",
			serverHandler: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					// Send response to be tested
					Body: ioutil.NopCloser(bytes.NewReader(testResp[:len(testResp)-4])),
					// Must be set to non-nil value or it panics
					Header: make(http.Header),
				}, nil
			},
			err: &json.SyntaxError{},
		},
		{
			name: "return error from server",
			serverHandler: func(req *http.Request) (*http.Response, error) {

				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					// Send response to be tested
					Body: ioutil.NopCloser(bytes.NewReader(testResp[:len(testResp)-4])),
					// Must be set to non-nil value or it panics
					Header: make(http.Header),
				}, nil
			},
			// rest error
			err: &Error{},
		},
	}
	for _, tt := range tcs {
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

func TestHTTPClient_GetJSONArray(t *testing.T) {
	tmp := []employee{
		{
			Name: "Joe",
			Job:  "accountant",
		},
		{
			Name: "Bob",
			Job:  "engineering",
		},
	}
	testResp, err := json.Marshal(tmp)
	var wantedArr []interface{}
	tmpEmp := make(map[string]interface{})
	tmpEmp["name"] = "Joe"
	tmpEmp["job"] = "accountant"
	tmpEmp2 := make(map[string]interface{})
	tmpEmp2["name"] = "Bob"
	tmpEmp2["job"] = "engineering"
	wantedArr = append(wantedArr, tmpEmp, tmpEmp2)
	assert.Nil(t, err)
	tcs := []struct {
		want          interface{}
		err           error
		serverHandler roundTripFunc
		name          string
	}{
		{
			name: "happy path, return success and response",
			serverHandler: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					// Send response to be tested
					Body: ioutil.NopCloser(bytes.NewReader(testResp)),
					// Must be set to non-nil value or it panics
					Header: make(http.Header),
				}, nil
			},
			want: wantedArr,
		},
		{
			name: "return invalid json",
			serverHandler: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					// Send response to be tested
					Body: ioutil.NopCloser(bytes.NewReader(testResp[:len(testResp)-4])),
					// Must be set to non-nil value or it panics
					Header: make(http.Header),
				}, nil
			},
			err: &json.SyntaxError{},
		},
		{
			name: "return error from server",
			serverHandler: func(req *http.Request) (*http.Response, error) {

				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					// Send response to be tested
					Body: ioutil.NopCloser(bytes.NewReader(testResp[:len(testResp)-4])),
					// Must be set to non-nil value or it panics
					Header: make(http.Header),
				}, nil
			},
			// rest error
			err: &Error{},
		},
	}
	for _, tt := range tcs {
		t.Run(tt.name, func(t *testing.T) {
			c := NewHTTPClient()
			setTransport(tt.serverHandler, c)
			ctx := context.Background()
			resp, err := c.GetJSONArray(ctx, "localhost:8080")
			if tt.err != nil {
				assert.IsType(t, tt.err, err)
				return
			}
			assert.Nil(t, err)
			assert.ElementsMatch(t, tt.want, resp)
		})
	}
}
