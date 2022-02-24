#!/bin/bash

# assumes that there is a running cluster with configured kubectl
kubectl delete -f k8s/hazelcast.yaml
kubectl delete -f k8s/deployment.yaml

# remove platform operator
kubectl delete -f https://repository.hazelcast.com/operator/bundle-5.0.yaml

