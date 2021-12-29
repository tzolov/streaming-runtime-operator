#!/bin/bash

set -euo pipefail

readonly CLIENT_GEN_DIR="/tmp/kubernetes-client-gen"
readonly CURRENT_KUBECTL_CONTEXT=$(kubectl config current-context)
readonly PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
echo $PROJECT_ROOT
readonly LOCAL_MANIFEST_FILE_1=$PROJECT_ROOT/streaming-runtime-crds/cluster-stream-crd.yaml
#readonly LOCAL_MANIFEST_FILE_2=$PROJECT_ROOT/streaming-runtime-crds/configuration-slice-crd.yaml

readonly STREAMING_PACKAGE=com/vmware/tanzu/streaming
readonly GENERATED_SOURCES_PATH=src/generated/java/$STREAMING_PACKAGE

createKindCluster() {
  # We need to ensure the kubernetes cluster is 1.19 in order to generate API classes see https://github.com/kubernetes-client/java/issues/1710
  kind create cluster --name crd-gen --image kindest/node:v1.19.11@sha256:cbecc517bfad65e368cd7975d1e8a4f558d91160c051d0b1d10ff81488f5fb06
  kubectl config use-context kind-crd-gen
}

cleanupKindCluster() {
  kind delete cluster --name crd-gen
  kubectl config use-context "$CURRENT_KUBECTL_CONTEXT"
}

deleteExisting() {
  kubectl delete -f "$LOCAL_MANIFEST_FILE_1" || true
#  kubectl delete -f "$LOCAL_MANIFEST_FILE_2" || true
  rm -Rf $CLIENT_GEN_DIR
  rm -Rf /tmp/gen-output
  rm -Rf /tmp/swagger
  rm -Rf "$PROJECT_ROOT"/streaming-runtime/$GENERATED_SOURCES_PATH/models/*
  rm -Rf "$PROJECT_ROOT"/streaming-runtime/$GENERATED_SOURCES_PATH/apis/*
}

applyCrd() {
  kubectl apply -f "$LOCAL_MANIFEST_FILE_1"
#  kubectl apply -f "$LOCAL_MANIFEST_FILE_2"
}

generate() {
  git clone https://github.com/kubernetes-client/gen ${CLIENT_GEN_DIR}|| true
  cd $CLIENT_GEN_DIR/openapi

  kubectl get --raw="/openapi/v2" > /tmp/swagger
#  while ! (grep -Fq '"ClusterConfigurationSource"' /tmp/swagger && grep -Fq '"ConfigurationSlice"' /tmp/swagger); do
  while ! (grep -Fq '"ClusterStream"' /tmp/swagger); do
    echo "Waiting for CRD to be applied..."
    sleep 1
    kubectl get --raw="/openapi/v2" > /tmp/swagger
  done

  bash java-crd-cmd.sh -n com.vmware.tanzu.streaming -p com.vmware.tanzu.streaming -l 2 -h true -o /tmp/gen-output -g true < /tmp/swagger
}

copyToProject() {
  cp -Rf /tmp/gen-output/src/main/java/$STREAMING_PACKAGE/models/ "$PROJECT_ROOT"/streaming-runtime/$GENERATED_SOURCES_PATH/models
  cp -Rf /tmp/gen-output/src/main/java/$STREAMING_PACKAGE/apis/   "$PROJECT_ROOT"/streaming-runtime/$GENERATED_SOURCES_PATH/apis
}

createKindCluster
deleteExisting
applyCrd
generate
copyToProject
cleanupKindCluster
