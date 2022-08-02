#!/bin/bash

# Delete allocated resources from the k8s
kubectl delete -f k8s/hazelcast.yaml
kubectl delete -f k8s/deployment.yaml
kubectl delete -f k8s/ingress.yaml
kubectl delete -f https://repository.hazelcast.com/operator/bundle-latest.yaml
