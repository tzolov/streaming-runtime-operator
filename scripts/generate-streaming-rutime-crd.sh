#!/bin/bash

set -euo pipefail

readonly PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"

base_generate -n com.vmware.tanzu.streaming \
 -p com.vmware.tanzu.streaming \
 -o "$PROJECT_ROOT"/streaming-runtime \
 -u $PROJECT_ROOT/streaming-runtime-crds/cluster-stream-crd.yaml
