## hz-go-service
- Hazelcast go client interaction on running hazelcast cluster in local cloud through minikube.
- Hazelcast go client can be tested local without kubernetes by making `localTest` true in `factory.go` source.

## Dependencies
- [Minikube](https://minikube.sigs.k8s.io/docs/start/)
- [Docker](https://www.docker.com/products/docker-desktop)

## Starting guide
Before starting, it is highly recommended using minikube on Docker Desktop. The guide supposes that Docker Desktop is ready and running properly. You can run it on other container and virtual machine environment, but it is not tested.
- `minikube start`
  - Start k8s environment on Docker Desktop as a container.
- `./start-deployments.sh`
  - Deploy necessary yamls to k8s.
- Make sure that hazelcast members are running on the cloud by checking
  - `kubectl logs pod/hazelcast-0`
- Forward internal k8s port to localhost
  - `kubectl port-forward service/hz-go-service 8080:8080`
  - First 8080 represents service port running inside k8s, the second one represents which port will be used for the local environment.

## Operations
- `curl localhost:8080/config`
  - Returns information about service.
- `curl 'localhost:8080/map?name=myDistributedMap&key=key1'`
  - Return the value of the entry that matched with key1
- `curl -H "Content-Type: application/json" --data-binary '{"value": "myValue"}' -X POST 'localhost:8080/map?name=myDistributedMap&key=key1'`
  - Change the value of existing entry of given map.
- `curl 'localhost:8080/map?name=myDistributedMap&key=key1'`
  - Return the updated value of the entry that matched with key1.

## Deleting sources from the k8s via script
- `./delete-deployments.sh`
  - Remove deployed sources from the k8s.

## Deleting sources via minikube and docker
Warning: Those instructions clear all your docker and minikube environment allocated resources.
- `minikube delete -all`
- `docker container rm $(docker ps -aq)`
  - Removes all running and paused containers from the docker.
  - If you want to delete only the container that is related to service, please get the ID of the running container then remove from the docker.
- `docker image rmi $(docker image ls -aq) -f`
  - Removes all images from the docker, again if you want to delete specific image related to minikube etc, you should learn the ID of the image and delete it.
