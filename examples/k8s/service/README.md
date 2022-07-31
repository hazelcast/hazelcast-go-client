# Running Hazelcast Go Client on Kubernetes Example
- This example shows how to run a sample proxy service which uses Hazelcast Go Client connected to the Hazelcast members in the same network and exposes itself to the outside through k8s ingress.

## Table of Contents
1. [Requirements](#requirements)
2. [Deployment](#deployment)
3. [Examples](#examples)
4. [Deletion of Kubernetes Resources](#deletion-of-kubernetes-resources)
5. [Running Service Locally](#running-service-locally)

<a name="requirements"></a>
## Requirements
- [Minikube](https://minikube.sigs.k8s.io/docs/start/)
- [Docker](https://www.docker.com/get-started/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

<a name="deployment"></a>
## Deployment  
The guide assumes that Docker Engine is ready and running properly. You can run it on other container and virtual machine environments, but it is not tested.
1. Open a new terminal session.
2. `minikube start --driver=docker`
3. `minikube docker-env`
4. `eval $(minikube -p minikube docker-env)`
5. Open another new terminal session.
   1. `minikube addons enable ingress`
   2. `sudo minikube tunnel`
6. Go back to previous session and continue on with `docker build -t example/hz-go-service .`
7. Add `127.0.0.1 hz-go-service.info` at the bottom of the `/etc/hosts` file.
8. `chmod +x start-deployment.sh`
9. Then, `./start-deployments.sh`
10. After that point, status of all k8s resources can be examined through: `kubectl get all`
11. `kubectl logs pod/hazelcast-sample-0`: checks whether Hazelcast members are up and running.
12. As a last step, lets ensure that the service is up and running by the help of ingress ip resolution:
13. `curl hz-go-service.info`

<a name="examples"></a>
## Examples
- Returns properties of the service and the Hazelcast Client as a json.
  ```bash
   ❯ curl hz-go-service.info/config
   {
      "ServiceConfig": {
        "ServiceName": "hazelcast-go-client-proxy-service",
        "Port": 8080,
        "Timeout": 5000000000
      },
      "HazelcastClientInfo": {
        "Name": "proxy-service-go-client",
        "Running": true,
        "MapSize": 3
      }
   }%
   ```
- Returns the value of the entry that matches with `key1`.
  ```bash
   ❯ curl 'hz-go-service.info/map?name=myDistributedMap&key=key1'
   {"Key":"key1","Value":"value1"}%
   ```
- Returns the value of the entry that matches with `key2`.
  ```bash
   ❯ curl 'hz-go-service.info/map?name=myDistributedMap&key=key2'
   {"Key":"key2","Value":"value2"}%
   ```
- Changes the value of existing entry that has `key1` as a key of the given map through POST request.
  ```bash
   ❯ curl -H "Content-Type: application/json" --data-binary '{"value": "myValue"}' -X POST 'hz-go-service.info/map?name=myDistributedMap&key=key1'
   Value: myValue has been put to map: myDistributedMap successfully.
   ```
- Returns the updated value of the entry that matches with `key1`.
  ```bash
   ❯ curl 'hz-go-service.info/map?name=myDistributedMap&key=key1'
   {"Key":"key1","Value":"myValue"}%
   ```

<a name="deletion-of-kubernetes-resources"></a>
## Deletion of Kubernetes Resources
- `./delete-deployments.sh`
- Turn of running Minikube tunnel.

<a name="running-service-locally"></a>
## Running Service Locally <a name="running-service-locally"></a>
1. Open a new terminal session.
2. `export HZ_GO_SERVICE_WITHOUT_K8S=true`
3. Open another new terminal session.
   1. `hz` CLI tool is need. For detailed information, [here](https://hazelcast.com/blog/hazelcast-command-line-is-released/). 
   2. Then, `hz start`: Starts a new Hazelcast cluster in local through CLI.
4. Go back to previous session and check out `Operations`.