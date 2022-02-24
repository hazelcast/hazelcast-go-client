package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"

	"github.com/nevzatseferoglu/hz-go-service/hz"
)

const (
	hzTimeout = time.Second * 3
	sampleMap = "myDistributedMap"
)

var (
	config    *ServiceConfig
	imdg      *InMemoryHzStorage
	hzc       *hazelcast.Client
	endpoints = [...]string{"config", "map"}
)

// Info return body json structure of request on map handler
type Info struct {
	Msg string `json:"msg"`
}

// InMemoryHzStorage Hazelcast storage used by the service.
type InMemoryHzStorage struct {
	myMap *hazelcast.Map
}

// ServiceConfig includes information about service configuration.
type ServiceConfig struct {
	ServiceName string        `json:"serviceName"`
	Port        int           `json:"port"`
	Timeout     time.Duration `json:"timeout"`
}

// getConfigHandler handles incoming config endpoint, returns status of service configs.
func getConfigHandler(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Add("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	serviceConfigResponse := struct {
		ServiceConfig
		hz.ClientInfo
	}{*config, hz.ClientInfo{ClientName: hzc.Name(), ClientRunning: hzc.Running(), MapSize: 0}}
	var err error
	serviceConfigResponse.ClientInfo.MapSize, err = imdg.myMap.Size(r.Context())
	if err != nil {
		http.Error(rw, fmt.Sprintf("Hazelcast map size, err: %v", err), http.StatusInternalServerError)
		return
	}
	rspJson, err := json.Marshal(serviceConfigResponse)
	if err != nil {
		http.Error(rw, fmt.Sprintf("Json parsing, err: %v", err), http.StatusBadRequest)
		return
	}
	_, err = rw.Write(rspJson)
	if err != nil {
		http.Error(rw, fmt.Sprintf("Could not write json data, err: %v", err), http.StatusInternalServerError)
		return
	}
}

// validateURLQueryParameter validates whether given request url query parameter is valid.
func validateURLQueryParameter(r *http.Request) (mapNames, keys []string, err error) {
	// resolve query parameters
	if err := r.ParseForm(); err != nil {
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

// mapEntryHandler make put and get operation on map.
func mapEntryHandler(rw http.ResponseWriter, r *http.Request) {
	// set response type
	rw.Header().Set("Content-Type", "application/json")
	var err error
	mapNames, keys, err := validateURLQueryParameter(r)
	if err != nil {
		http.Error(rw, fmt.Sprintf("URL query parameters cannot be validated, err: %v", err), http.StatusBadRequest)
		return
	}
	mapName := mapNames[0]
	key := keys[0]
	// wrap a timeout context for the hazelcast operation
	ctx, cancel := context.WithTimeout(r.Context(), hzTimeout)
	defer cancel()
	objInfo, err := hzc.GetDistributedObjectsInfo(ctx)
	if err != nil {
		http.Error(rw, "Hazelcast objects info cannot be obtained", http.StatusInternalServerError)
		return
	}
	exist := false
	for _, o := range objInfo {
		if hazelcast.ServiceNameMap == o.ServiceName && mapName == o.Name {
			exist = true
			break
		}
	}
	switch r.Method {
	case "GET":
		rw.WriteHeader(http.StatusOK)
		// return proper response as a json value
		var rsp []byte
		if !exist {
			nonExistMap := Info{
				Msg: "There is no map for the given name",
			}
			rsp, err = json.Marshal(nonExistMap)
			if err != nil {
				http.Error(rw, fmt.Sprintf("marshal cannot work properly for non exist map response, rsp: %v\n", nonExistMap), http.StatusInternalServerError)
				return
			}
			_, err = rw.Write(rsp)
			return
		}
		// get map
		m, err := hzc.GetMap(ctx, mapName)
		if err != nil {
			http.Error(rw, fmt.Sprintf("Map: %s cannot be returned from the hazelcast", mapName), http.StatusInternalServerError)
			return
		}
		// get value
		value, err := m.Get(ctx, key)
		if err != nil {
			http.Error(rw, fmt.Sprintf("Value of key: %s cannot be returned from the hazelcast", key), http.StatusInternalServerError)
			return
		}
		if value == nil {
			nonExist := Info{
				Msg: "There is no value for the given key",
			}
			rsp, err = json.Marshal(nonExist)
			if err != nil {
				http.Error(rw, fmt.Sprintf("Marshal cannot work properly for non exist value response, rsp: %v", nonExist), http.StatusInternalServerError)
				return
			}
		} else {
			entry := types.Entry{
				Key:   key,
				Value: value,
			}
			rsp, err = json.Marshal(entry)
			if err != nil {
				http.Error(rw, fmt.Sprintf("Marshal cannot work properly for the existing value response, rsp: %v", entry), http.StatusInternalServerError)
				return
			}
		}
		_, err = rw.Write(rsp)
		if err != nil {
			http.Error(rw, fmt.Sprintf("Response cannot be written properly"), http.StatusInternalServerError)
			return
		}
	case "POST":
		rw.WriteHeader(http.StatusCreated)
		if !isContentTypeJson(r) {
			http.Error(rw, "Invalid content type is given in header", http.StatusBadRequest)
			return
		}
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(rw, "response body cannot be read", http.StatusBadRequest)
			return
		}
		type RequestBody struct {
			Value interface{} `json:"value"`
		}
		var reqBody RequestBody
		err = json.Unmarshal(body, &reqBody)
		if err != nil {
			http.Error(rw, "Invalid request body", http.StatusBadRequest)
			return
		}
		var rsp []byte
		pmap, err := hzc.GetMap(ctx, mapName)
		if err != nil {
			http.Error(rw, "Hazelcast cannot return a map", http.StatusInternalServerError)
			return
		}
		_, err = pmap.Put(ctx, key, reqBody.Value)
		if err != nil {
			http.Error(rw, fmt.Sprintf("Hazelcast cannot put %v value to map", reqBody.Value), http.StatusInternalServerError)
			return
		}
		successInfo := Info{
			Msg: fmt.Sprintf("(Key: %s, Value: %v) has been put to Map: %s successfully",
				key, reqBody.Value, mapName),
		}
		rsp, err = json.Marshal(successInfo)
		if err != nil {
			http.Error(rw, fmt.Sprintf("Marshal cannot work properly for non exist map put, rsp: %v", successInfo), http.StatusInternalServerError)
			return
		}
		_, err = rw.Write(rsp)
		if err != nil {
			http.Error(rw, fmt.Sprintf("Response cannot be written properly"), http.StatusInternalServerError)
			return
		}
	}
}

// isContentTypeJson determine whether content-type of the request header is in json type.
func isContentTypeJson(r *http.Request) bool {
	contentType := r.Header.Get("Content-type")
	if contentType == "" {
		return false
	}
	if contentType != "application/json" {
		return false
	}
	return true
}

// healthHandler handlers for k8s pod health.
func healthHandler(rw http.ResponseWriter, _ *http.Request) {
	rw.WriteHeader(http.StatusOK)
}

// readinessHandler handlers for k8s pod readiness.
func readinessHandler(rw http.ResponseWriter, _ *http.Request) {
	rw.WriteHeader(http.StatusOK)
}

// newDefaultServiceConfig returns new config.
func newDefaultServiceConfig() *ServiceConfig {
	// default config properties
	return &ServiceConfig{
		ServiceName: "hz-go-service",
		Port:        8080,
		Timeout:     5 * time.Second,
	}
}

// newInMemoryHzMap creates a sample map and put example entries on it.
func newInMemoryHzMap(ctx context.Context, c *hazelcast.Client) (*InMemoryHzStorage, error) {
	entries := []types.Entry{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
	}
	newMap, err := c.GetMap(ctx, sampleMap)
	err = newMap.PutAll(ctx, entries...)
	if err != nil {
		return nil, err
	}
	return &InMemoryHzStorage{myMap: newMap}, nil
}

// newServer initiates new server instance according to given router and config.
func newServer(router *mux.Router, config *ServiceConfig) *http.Server {
	return &http.Server{
		Handler:      router,
		Addr:         fmt.Sprintf(":%d", config.Port),
		ReadTimeout:  config.Timeout,
		WriteTimeout: config.Timeout,
	}
}

// newRouter returns a new mux router includes certain handlers.
func newRouter() *mux.Router {
	// creates a server
	router := mux.NewRouter()
	// health checks for kubernetes
	router.HandleFunc("/health", healthHandler)
	router.HandleFunc("/readiness", readinessHandler)
	// service configuration handler
	router.HandleFunc(fmt.Sprintf("/%s", endpoints[0]), getConfigHandler)
	// hazelcast map handlers
	router.HandleFunc(fmt.Sprintf("/%s", endpoints[1]), mapEntryHandler)
	return router
}

func main() {
	ctx := context.Background()
	// Main error agent
	var err error
	// Set service config as default
	config = newDefaultServiceConfig()
	// Initiate a new client
	hzc, err = hz.NewHzClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Create a new in-memory storage
	imdg, err = newInMemoryHzMap(ctx, hzc)
	if err != nil {
		log.Fatal(err)
	}
	// Initiate new route with registered HandlerFunc
	router := newRouter()
	// Create server instance
	srv := newServer(router, config)
	// Start http server
	go func() {
		log.Println("Server is listening...")
		log.Fatal(srv.ListenAndServe())
	}()
	// Handle incoming signals
	handleSignal(ctx, srv)
}

// handleSignal handles incoming SIGINT amd SIGTERM  signals, shutdown gracefully.
func handleSignal(ctx context.Context, srv *http.Server) {
	// Initiate signal channel
	signalChan := make(chan os.Signal, 1)
	// Relay specified signals to channel when they are sent
	signal.Notify(signalChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	// Block until receiving signals
	<-signalChan
	// New context for limit the closing time
	ctx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	// Shutdown hazelcast client
	if hzc != nil {
		err := hzc.Shutdown(ctx)
		if err != nil {
			log.Fatalf("Client cannot be shut down after the signal is received, err: %v\n", err)
		}
	}
	// Shutdown server properly
	err := srv.Shutdown(ctx)
	if err != nil {
		log.Fatalf("Server cannot be properly shut down after the signal is received, err: %v\n", err)
	}
	log.Println("Server has been shut down!")
}
