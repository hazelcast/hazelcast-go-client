# Running Hazelcast Go Client on Kubenetes Example
- This example shows how to run an application which uses Hazelcast Go Client on Kubernetes.

## Requirements
- [Minikube](https://minikube.sigs.k8s.io/docs/start/)

## Starting guide
Before starting, it is highly recommended using minikube on Docker Desktop. The guide supposes that Docker Desktop is ready and running properly. You can run it on other container and virtual machine environment, but it is not tested.
1. `minikube start`: Start deploying to k8s.
2. `./start-deployments.sh`: Deploy necessary yamls to k8s.
3. `kubectl logs pod/hazelcast-0`: After a while, check whether Hazelcast members are up and running.
4. `kubectl port-forward service/hz-go-service 8080:8080`: Forward internal k8s port to localhost. `8080:8080` maps service port to host port.

## Operations
- `curl localhost:8080/config`: Returns information about service.
- `curl 'localhost:8080/map?name=myDistributedMap&key=key1'`: Return the value of the entry that matched with `key1`.
- `curl -H "Content-Type: application/json" --data-binary '{"value": "myValue"}' -X POST 'localhost:8080/map?name=myDistributedMap&key=key1'`: Change the value of existing entry of given map.
- `curl 'localhost:8080/map?name=myDistributedMap&key=key1'`: Return the updated value of the entry that matched with key1.

## Deleting sources from the k8s via script
- `./delete-deployments.sh`: Remove deployed objects from the k8s.

## Deleting sources via minikube and docker
Warning: Those instructions clear all your docker and minikube environment allocated resources.
1. `minikube delete -all`
2. `docker container rm $(docker ps -aq)`
  - Removes all running and paused containers from the docker.
  - If you want to delete only the container that is related to service, please get the ID of the running container then remove from the docker.
  - `docker image rmi $(docker image ls -aq) -f`: Removes all images from the docker, again if you want to delete specific image related to minikube etc, you should learn the ID of the image and delete it.

## Testing go client without kubernetes deployment
- `localTest` true in `factory.go` source.