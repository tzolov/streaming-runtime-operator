#!/bin/bash

set -euo pipefail

readonly PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"

bash "$PROJECT_ROOT"/scripts/generate.sh \
  -n com.vmware.tanzu.streaming \
  -p com.vmware.tanzu.streaming \
  -o "$PROJECT_ROOT"/streaming-runtime \
  -u "$PROJECT_ROOT"/streaming-runtime-crds/stream-crd.yaml \
  -u "$PROJECT_ROOT"/streaming-runtime-crds/cluster-stream-crd.yaml \
# -u "$PROJECT_ROOT"/streaming-runtime-crds/processor-crd.yaml
