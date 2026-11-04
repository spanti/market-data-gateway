FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src

# Install Python dependencies from pyproject.toml and update pip
RUN pip install --upgrade pip && \
    pip install --index-url https://pypi.org/simple/ .

COPY config ./config
RUN mkdir -p /app/data

EXPOSE 8080 8081 8082 5555 5556 5557 5558

# The image default is the all-in-one supervisor; docker-compose overrides the
# command to run one Service Role per container.
CMD ["mdgw", "up", "--config", "config/gateway.docker.toml"]
