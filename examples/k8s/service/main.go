/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/mux"

	"github.com/nevzatseferoglu/hz-go-service/util"
)

// NewHttpServer creates new server instance according to given router and service config.
func NewHttpServer(router *mux.Router, serviceConfig *util.ServiceConfig) *http.Server {
	return &http.Server{
		Handler:      router,
		Addr:         fmt.Sprintf(":%d", serviceConfig.Port),
		ReadTimeout:  serviceConfig.Timeout,
		WriteTimeout: serviceConfig.Timeout,
	}
}

// NewRouter returns a new mux router includes certain handlers.
func NewRouter(service *util.Service) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/", service.RootHandler)
	okHandler := func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}
	router.HandleFunc("/health", okHandler)
	router.HandleFunc("/readiness", okHandler)
	router.HandleFunc("/config", service.ConfigHandler).Methods(http.MethodGet)
	router.HandleFunc("/map", service.MapGetHandler).Methods(http.MethodGet)
	router.HandleFunc("/map", service.MapPutHandler).Methods(http.MethodPost)
	return router
}

func main() {
	ctx := context.Background()
	service, err := util.NewDefaultService(ctx)
	if err != nil {
		log.Fatal(err)
	}
	server := NewHttpServer(NewRouter(service), service.ServiceConfig)
	log.Println("Server is up and listening...")
	go func() {
		log.Fatal(server.ListenAndServe())
	}()
	handleSignal(ctx, server, service)
}

// handleSignal handles incoming SIGINT and SIGTERM signals then shutdown service gracefully.
func handleSignal(ctx context.Context, server *http.Server, service *util.Service) {
	var err error
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	s := <-signalChan
	log.Printf("Signal %v has been received, server is shutting down...", s)
	ctx, cancel := context.WithTimeout(ctx, service.ServiceConfig.Timeout)
	defer cancel()
	if err = service.Client.Shutdown(ctx); err != nil {
		log.Printf("Client cannot be shut down propertly after the signal is received, err: %v\n", err)
	}
	if err = server.Shutdown(ctx); err != nil {
		log.Printf("Server cannot be shut down properly after the signal is received, err: %v\n", err)
	}
}
