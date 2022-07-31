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
	router.HandleFunc(fmt.Sprintf("/%s", util.RootEndpoints[0]), service.ConfigHandler).Methods(http.MethodGet)
	router.HandleFunc(fmt.Sprintf("/%s", util.RootEndpoints[1]), service.MapGetHandler).Methods(http.MethodGet)
	router.HandleFunc(fmt.Sprintf("/%s", util.RootEndpoints[1]), service.MapPutHandler).Methods(http.MethodPost)
	return router
}

// toDo
// change the name of the k8s resources
// rewrite readme according to comments and final source
func main() {
	ctx := context.Background()
	service, err := util.NewDefaultService(ctx)
	if err != nil {
		log.Fatal(err)
	}
	server := NewHttpServer(NewRouter(service), service.ServiceConfig)
	log.Println("Server is up and listening...")
	//go log.Fatal(server.ListenAndServe())
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
