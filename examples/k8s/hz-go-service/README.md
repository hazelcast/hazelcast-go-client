# Running Hazelcast Go Client on Kubernetes Example
- This example shows how to run a sample proxy service which uses Hazelcast go client connected to the Hazelcast members in the same network and expose itself outside through k8s ingress.

## Requirements
- [Minikube](https://minikube.sigs.k8s.io/docs/start/)
- [Docker](https://www.docker.com/get-started/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

## Start deployment
Before the start, it is highly recommended using minikube on Docker Desktop. The guide supposes that Docker Desktop is ready and running properly. You can run it on other container and virtual machine environment, but it is not tested.
1. Open a new terminal session and type specified command one by one.
2. `minikube start --driver`
3. `minikube docker-env`
4. `eval $(minikube -p minikube docker-env)`
5. Open a new terminal session
   1. `minikube addons enable ingress`
   2. `sudo minikube tunnel`:
6. Go back to previous session and continue on `docker build -t example/hz-go-service .`
7. Add `127.0.0.1 hz-go-service.info` at the bottom of the `/etc/hosts`
8. `./start-deployments.sh`:
9. After that point, you can view the status of all k8s resources through: `kubectl get all`
10. `kubectl logs pod/hazelcast-sample-0`: For example, checks whether Hazelcast members are up and running.
11. At last step, lets ensure that our service is up and running by the help of ingress ip resolution:
12. `curl hz-go-service.info`

## Operations
- `curl hz-go-service.info/config`: Returns information about service.
- `curl 'hz-go-service.info/map?name=myDistributedMap&key=key1'`: Returns the value of the entry that matched with `key1`.
- `curl -H "Content-Type: application/json" --data-binary '{"value": "myValue"}' -X POST 'hz-go-service.info/map?name=myDistributedMap&key=key1'`: Changes the value of existing entry of given map.
- `curl 'hz-go-service.info/map?name=myDistributedMap&key=key1'`: Returns the updated value of the entry that matched with key1.

## Deleting resources from the k8s
- `./delete-deployments.sh`: Removes deployed k8s resources.

## Deleting all sources from minikube and docker
Warning: Those instructions clear all your docker and minikube environment allocated resources.
1. `minikube delete -all`
2. `docker container rm $(docker ps -aq)`
  - Removes all running and paused containers from the docker.
  - If you want to delete only the container that is related to service, please get the ID of the running container then remove from the docker.
  - `docker image rmi $(docker image ls -aq) -f`: Removes all images from the docker, again if you want to delete specific image related to minikube etc, you should learn the ID of the image and delete it.

## Testing go client without kubernetes deployment
- `localTest` true in `factory.go` source.