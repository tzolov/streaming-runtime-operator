#!/bin/bash
# Copyright 2020 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script generates the model classes from a released version of cert-manager CRDs
# under src/main/java/io/cert/manager/models.

#DEFAULT_IMAGE_NAME=docker.pkg.github.com/kubernetes-client/java/crd-model-gen
DEFAULT_IMAGE_NAME=ghcr.io/kubernetes-client/java/crd-model-gen
DEFAULT_IMAGE_TAG=v1.0.6
IMAGE_NAME=${IMAGE_NAME:=$DEFAULT_IMAGE_NAME}
IMAGE_TAG=${IMAGE_TAG:=$DEFAULT_IMAGE_TAG}

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"

# a crdgen container is run in a way that:
#   1. it has access to the docker daemon on the host so that it is able to create sibling container on the host
#   2. it runs on the host network so that it is able to communicate with the KinD cluster it launched on the host
docker run \
  --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd)":"$(pwd)" \
  --network host \
  ${IMAGE_NAME}:${IMAGE_TAG} \
  /generate.sh \
  -u $PROJECT_ROOT/streaming-runtime-crds/cluster-stream-crd.yaml \
  -n com.vmware.tanzu.streaming \
  -p com.vmware.tanzu.streaming \
  -o "$(pwd)"