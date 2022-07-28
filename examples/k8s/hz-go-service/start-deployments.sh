#!/bin/bash

# assumes that there is a running cluster with configured kubectl
kubectl apply -f https://repository.hazelcast.com/operator/bundle-latest.yaml

# deploy hazelcast through operator
kubectl apply -f k8s/hazelcast.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/ingress.yaml