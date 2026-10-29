#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
context=kind-viro
case "${1:-deploy}" in
  create)
    if ! kind get clusters | grep -qx viro; then
      kind create cluster --name viro --wait 180s
    fi
    kubectl --context "$context" apply -f k8s/infra.yaml
    ;;
  deploy)
    kind load docker-image viro:local --name viro
    kubectl --context "$context" apply -f k8s/infra.yaml
    kubectl --context "$context" -n viro rollout status statefulset/kafka --timeout=180s
    kubectl --context "$context" -n viro create configmap viro-source --from-file=observations.json=data/observations.json --dry-run=client -o yaml | kubectl --context "$context" apply -f -
    kubectl --context "$context" apply -f k8s/apps.yaml
    kubectl --context "$context" -n viro rollout status statefulset/viro-worker --timeout=180s
    kubectl --context "$context" -n viro rollout status deployment/viro-gateway --timeout=180s
    kubectl --context "$context" apply -f k8s/load.yaml
    kubectl --context "$context" -n viro wait --for=condition=complete job/viro-load --timeout=180s
    ;;
  status)
    kubectl --context "$context" -n viro get pods,pvc,svc,jobs
    ;;
  gateway)
    kubectl --context "$context" -n viro port-forward svc/viro-gateway 8080:8080
    ;;
  metrics)
    kubectl --context "$context" -n viro port-forward svc/prometheus 9090:9090
    ;;
  *)
    echo 'Usage: scripts/cluster.sh {create|deploy|status|gateway|metrics}' >&2
    exit 2
    ;;
esac
