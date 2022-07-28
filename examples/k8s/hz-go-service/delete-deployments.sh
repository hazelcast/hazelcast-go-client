#!/bin/bash

# assumes that there is a running cluster with configured kubectl
kubectl delete -f k8s/hazelcast.yaml
kubectl delete -f k8s/deployment.yaml
kubectl apply -f k8s/ingress.yaml

# remove platform operator
kubectl delete -f https://repository.hazelcast.com/operator/bundle-latest.yaml

