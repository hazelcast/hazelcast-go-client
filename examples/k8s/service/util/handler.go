package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"
)

var (
	// RootEndpoints defines the valid root path of the service.
	RootEndpoints = [...]string{"config", "map"}
)

// ConfigHandler handles the request to config endpoint, returns properties of service and client.
func (s *Service) ConfigHandler(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Add("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	var err error
	var mapSize int
	if mapSize, err = s.ExampleMap.Size(r.Context()); err != nil {
		http.Error(rw, fmt.Sprintf("Hazelcast map size, err: %v", err), http.StatusInternalServerError)
		return
	}
	configHandlerResponse := struct {
		Config *ServiceConfig       `json:"ServiceConfig"`
		Hci    *HazelcastClientInfo `json:"HazelcastClientInfo"`
	}{
		Config: s.ServiceConfig,
		Hci: &HazelcastClientInfo{
			Name:    s.Client.Name(),
			Running: s.Client.Running(),
			MapSize: mapSize,
		},
	}
	var rsp []byte
	if rsp, err = json.MarshalIndent(configHandlerResponse, "", "    "); err != nil {
		http.Error(rw, fmt.Sprintf("Json parsing, err: %v", err), http.StatusBadRequest)
		return
	}
	if _, err = rw.Write(rsp); err != nil {
		http.Error(rw, fmt.Sprintf("Could not write json data, err: %v", err), http.StatusInternalServerError)
		return
	}
}

// RootHandler handles the root path.
func (s *Service) RootHandler(rw http.ResponseWriter, _ *http.Request) {
	rw.WriteHeader(http.StatusOK)
	if _, err := rw.Write([]byte("Go service is alive!\n")); err != nil {
		http.Error(rw, fmt.Sprintf("Response cannot be written properly"), http.StatusInternalServerError)
		return
	}
}

// MapGetHandler handles incoming GET request to map.
func (s *Service) MapGetHandler(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	mapNames, keys, err := validateURLQueryParameter(r)
	if err != nil {
		http.Error(rw, fmt.Sprintf("URL query parameters cannot be validated, err: %v", err), http.StatusBadRequest)
		return
	}
	mapName, key := mapNames[0], keys[0]
	rw.WriteHeader(http.StatusOK)
	ctx, cancel := context.WithTimeout(r.Context(), s.ServiceConfig.Timeout)
	defer cancel()
	var m *hazelcast.Map
	if m, err = s.Client.GetMap(ctx, mapName); err != nil {
		http.Error(rw, fmt.Sprintf("Map: %s cannot be returned from the Hazelcast cluster", mapName), http.StatusInternalServerError)
		return
	}
	var value interface{}
	if value, err = m.Get(ctx, key); err != nil {
		http.Error(rw, fmt.Sprintf("Value of key: %s cannot be returned from the Hazalcast cluster", key), http.StatusInternalServerError)
		return
	}
	entry := types.Entry{
		Key:   key,
		Value: value,
	}
	var rsp []byte
	if rsp, err = json.Marshal(entry); err != nil {
		http.Error(rw, fmt.Sprintf("Marshal cannot work properly for the existing value response, rsp: %v", entry), http.StatusInternalServerError)
		return
	}
	if _, err = rw.Write(rsp); err != nil {
		http.Error(rw, fmt.Sprintf("Response cannot be written properly"), http.StatusInternalServerError)
		return
	}
}

// MapPutHandler handles incoming POST request to map.
func (s *Service) MapPutHandler(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	mapNames, keys, err := validateURLQueryParameter(r)
	if err != nil {
		http.Error(rw, fmt.Sprintf("URL query parameters cannot be validated, err: %v", err), http.StatusBadRequest)
		return
	}
	mapName, key := mapNames[0], keys[0]
	if !isContentTypeHeaderJSON(r) {
		http.Error(rw, "Invalid content type is given in the header", http.StatusBadRequest)
		return
	}
	rw.WriteHeader(http.StatusCreated)
	var body []byte
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		http.Error(rw, "response body cannot be read", http.StatusBadRequest)
		return
	}
	var reqBody struct {
		Value interface{} `json:"value"`
	}
	if err = json.Unmarshal(body, &reqBody); err != nil {
		http.Error(rw, "Invalid request body", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.ServiceConfig.Timeout)
	defer cancel()
	var m *hazelcast.Map
	if m, err = s.Client.GetMap(r.Context(), mapName); err != nil {
		http.Error(rw, fmt.Sprintf("Map: %s cannot be returned from the Hazelcast cluster", mapName), http.StatusInternalServerError)
		return
	}
	if _, err = m.Put(ctx, key, reqBody.Value); err != nil {
		http.Error(rw, fmt.Sprintf("Value %v cannot be put to map %v", reqBody.Value, mapName), http.StatusInternalServerError)
		return
	}
	if _, err = rw.Write([]byte(fmt.Sprintf("Value: %v has been put to map: %s successfully.\n", reqBody.Value, mapName))); err != nil {
		http.Error(rw, fmt.Sprintf("Response cannot be written properly"), http.StatusInternalServerError)
		return
	}
}

func isContentTypeHeaderJSON(r *http.Request) bool {
	contentType := r.Header.Get("Content-type")
	if contentType == "" {
		return false
	}
	if contentType != "application/json" {
		return false
	}
	return true
}

func validateURLQueryParameter(r *http.Request) (mapNames, keys []string, err error) {
	if err = r.ParseForm(); err != nil {
		return nil, nil, err
	}
	var ok bool
	if mapNames, ok = r.Form["name"]; !ok {
		return nil, nil, errors.New("there is no query parameter as name")
	}
	if keys, ok = r.Form["key"]; !ok {
		return nil, nil, errors.New("there is no query parameter as key")
	}
	return mapNames, keys, nil
}
