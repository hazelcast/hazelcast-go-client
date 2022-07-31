#!/bin/bash

# Creates k8s resources according to given sources.
kubectl apply -f https://repository.hazelcast.com/operator/bundle-latest.yaml
kubectl apply -f k8s/hazelcast.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/ingress.yaml