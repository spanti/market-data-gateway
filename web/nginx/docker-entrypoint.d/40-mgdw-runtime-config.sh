#!/bin/sh
# Select and render the Nginx site configuration for MDGW_RUNTIME_MODE.
#
# The mode is read at container start, never baked into the image. Only the
# four upstream variables are substituted; nginx runtime variables such as
# $host and $remote_addr are left untouched.
set -eu

mode="${MDGW_RUNTIME_MODE:-current-contract}"
case "$mode" in
  current-contract|admin-api) ;;
  *)
    echo "mdgw: invalid MDGW_RUNTIME_MODE='$mode' (expected current-contract or admin-api)" >&2
    exit 1
    ;;
esac

: "${MDGW_API_UPSTREAM:=http://api:8080}"
: "${MDGW_COLLECTOR_OPS_UPSTREAM:=http://collector:8081}"
: "${MDGW_REPLAY_OPS_UPSTREAM:=http://replay:8082}"
: "${MDGW_PROMETHEUS_UPSTREAM:=http://prometheus:9090}"
# Docker's embedded DNS. Upstream names are resolved per request, so an
# optional service (for example Prometheus) may be absent without preventing
# the static container from starting.
: "${MDGW_DNS_RESOLVER:=127.0.0.11}"
export MDGW_API_UPSTREAM MDGW_COLLECTOR_OPS_UPSTREAM
export MDGW_REPLAY_OPS_UPSTREAM MDGW_PROMETHEUS_UPSTREAM MDGW_DNS_RESOLVER

template="/etc/nginx/mgdw/${mode}.conf.template"
if [ ! -f "$template" ]; then
  echo "mdgw: missing config template $template" >&2
  exit 1
fi

envsubst \
  '${MDGW_API_UPSTREAM} ${MDGW_COLLECTOR_OPS_UPSTREAM} ${MDGW_REPLAY_OPS_UPSTREAM} ${MDGW_PROMETHEUS_UPSTREAM} ${MDGW_DNS_RESOLVER}' \
  < "$template" > /etc/nginx/conf.d/default.conf

echo "mdgw: runtime mode=${mode} api=${MDGW_API_UPSTREAM}"
if [ "$mode" = "current-contract" ]; then
  echo "mdgw: WARNING Current Contract is an unauthenticated read-only service; deploy it on a trusted intranet only."
fi
