# Streaming Runtime Operator
POC
## Quick Start

#### Starting Minikube

You can get [here](https://kubernetes.io/docs/tasks/tools/#installation) latest minikube binary.

```shell
minikube start --memory=8196 --cpus 8
```

#### Applying SR installation file
Next we apply the StreamingRuntime (SR) install files, including ClusterRoles, 
ClusterRoleBindings and some Custom Resource Definitions (CRDs) for declarative management of 
`ClusterStreams`, `Streams` and `Processors`.

```shell
kubectl apply -f 'https://raw.githubusercontent.com/tzolov/streaming-runtime-operator/main/install.yaml' -n streaming-runtime
```

#### Provision a simple streaming pipeline

```shell
kubectl apply -f 'https://raw.githubusercontent.com/tzolov/streaming-runtime-operator/main/samples/test-all.yaml' -n streaming-runtime
```

