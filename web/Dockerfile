# Production image for the Market Data Gateway Operator Console.
#
# Stage 1 builds the static bundle with the committed lockfile. Stage 2 is a
# plain Nginx static runtime: no Node process, no npm, no source tree.
#
# Build:
#   docker build -t mdgw-operator-console:local .
# Run (Current Contract, same-origin proxy):
#   docker run --rm -p 8080:80 \
#     -e MDGW_RUNTIME_MODE=current-contract \
#     -e MDGW_API_UPSTREAM=http://host.docker.internal:8080 \
#     mdgw-operator-console:local
#
# MDGW_RUNTIME_MODE is resolved at container start (not build time) and
# selects the proxy template: current-contract | admin-api.

FROM node:22-alpine AS build
WORKDIR /app

# Copy manifests first so dependency installation is cached independently of
# application source. `npm ci` fails if package-lock.json is missing/stale.
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM nginx:1.27-alpine AS runtime

LABEL org.opencontainers.image.title="Market Data Gateway Operator Console"
LABEL org.opencontainers.image.description="Static React console served by Nginx with a same-origin API proxy"

COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx/ /etc/nginx/mgdw/
COPY nginx/docker-entrypoint.d/40-mgdw-runtime-config.sh /docker-entrypoint.d/40-mgdw-runtime-config.sh
RUN chmod +x /docker-entrypoint.d/40-mgdw-runtime-config.sh

# Runtime settings are explicit. Upstreams must resolve from inside the
# container at start time (Compose service names or reachable hosts).
ENV MDGW_RUNTIME_MODE=current-contract \
    MDGW_API_UPSTREAM=http://api:8080 \
    MDGW_COLLECTOR_OPS_UPSTREAM=http://collector:8081 \
    MDGW_REPLAY_OPS_UPSTREAM=http://replay:8082 \
    MDGW_PROMETHEUS_UPSTREAM=http://prometheus:9090 \
    MDGW_DNS_RESOLVER=127.0.0.11

EXPOSE 80

# The static shell is the container's job; API readiness is a backend concern.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1/ || exit 1
