FROM python:3.13-slim

# Builds behind a firewall or a slow link to pypi.org can point this at a
# mirror without editing the image, e.g.
#   docker compose build --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_INDEX_URL=https://pypi.org/simple/

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src

# Install Python dependencies from pyproject.toml and update pip
RUN pip install --upgrade pip && \
    pip install --index-url "$PIP_INDEX_URL" .

COPY config ./config
RUN mkdir -p /app/data

EXPOSE 8080 8081 8082 5555 5556 5557 5558

# The image default is the all-in-one supervisor; docker-compose overrides the
# command to run one Service Role per container.
CMD ["mdgw", "up", "--config", "config/gateway.docker.toml"]
