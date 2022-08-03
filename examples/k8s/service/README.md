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
1. Open a new terminal session and type the following commands one by one in a row to observe the service. 
2. `git clone https://github.com/hazelcast/hazelcast-go-client.git`
3. `cd hazelcast-go-client/examples/k8s/service`
4. `minikube start --driver=docker`
5. `minikube docker-env`
6. `eval $(minikube -p minikube docker-env)`
7. Open another new terminal session.
   1. `minikube addons enable ingress`
   2. `sudo minikube tunnel`
8. Go back to previous session and continue on with `docker build -t example/hz-go-service .`
9. Add `127.0.0.1 hz-go-service.info` at the bottom of the `/etc/hosts` file.
10. `chmod +x start-deployments.sh`
11. Then, `./start-deployments.sh`
12. After that point, status of all k8s resources can be examined through: `kubectl get all`
13. `kubectl logs pod/hazelcast-sample-0`: checks whether Hazelcast members are up and running.
    - **If you cannot see the logs or getting not RUNNING state for the k8s resources through kubectl, just wait for a few seconds and reapply the command.**  
14. As a last step, lets ensure that the service is up and running by the help of ingress ip resolution:
15. `curl hz-go-service.info`

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
1. `chmod +x delete-deployments.sh`
2. `./delete-deployments.sh`
3. Turn of running Minikube tunnel.

<a name="running-service-locally"></a>
## Running Service Locally <a name="running-service-locally"></a>
1. Open a new terminal session.
2. `export HZ_GO_SERVICE_WITHOUT_K8S=1`
3. Open another new terminal session.
   1. `hz` CLI tool is need. For detailed information, [here](https://hazelcast.com/blog/hazelcast-command-line-is-released/). 
   2. Then, `hz start`: Starts a new Hazelcast cluster in local through CLI.
4. Go back to the previous terminal session and check out `Examples` section in README.md.
5. Don't forget to replace `hz-go-service.info` with `localhost:8080` before applying request.