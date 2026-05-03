# syntax=docker/dockerfile:1.7

FROM debian:trixie-slim AS base
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    curl tini ca-certificates postgresql-client && \
    rm -rf /var/lib/apt/lists/*

FROM golang:1.25-trixie AS build-from-source
ENV DEBIAN_FRONTEND=noninteractive
ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN mkdir -p /tmp/doltgresql/
COPY . /tmp/doltgresql/
WORKDIR /tmp/doltgresql/

RUN set -eux; \
    if [ "$TARGETOS" != "linux" ]; then \
      echo "Docker images can only be built for linux targets, got $TARGETOS/$TARGETARCH"; \
      exit 1; \
    fi; \
    go mod download; \
    ./postgres/parser/build.sh; \
    ./scripts/build_binaries.sh "$TARGETOS-$TARGETARCH"; \
    install -m 0755 "out/doltgresql-$TARGETOS-$TARGETARCH/bin/doltgres" /usr/local/bin/doltgres

FROM base AS download-binary
ARG DOLTGRES_VERSION="latest"
ARG DOLTGRES_RELEASE_REPO="dolthub/doltgresql"

RUN --mount=type=secret,id=github_token,required=false set -eux; \
    if [ "$DOLTGRES_VERSION" = "source" ]; then \
      echo "DOLTGRES_VERSION=source requires --target runtime-source"; \
      exit 1; \
    fi; \
    github_curl() { \
      if [ -s /run/secrets/github_token ]; then \
        curl -fsSL -H "Authorization: Bearer $(cat /run/secrets/github_token)" "$@"; \
      else \
        curl -fsSL "$@"; \
      fi; \
    }; \
    if [ "$DOLTGRES_VERSION" = "latest" ]; then \
      version=$(github_curl "https://api.github.com/repos/${DOLTGRES_RELEASE_REPO}/releases/latest" \
        | grep '"tag_name"' \
        | cut -d'"' -f4 \
        | sed 's/^v//'); \
    else \
      version="${DOLTGRES_VERSION#v}"; \
    fi; \
    install_url="https://github.com/${DOLTGRES_RELEASE_REPO}/releases/download/v${version}/install.sh"; \
    echo "fetching ${install_url}"; \
    github_curl "$install_url" | bash

FROM base AS runtime-base
ARG DOLTGRES_IMAGE_SOURCE="https://github.com/dolthub/doltgresql"
ARG DOLTGRES_IMAGE_REVISION=""
ARG DOLTGRES_IMAGE_VERSION=""

LABEL org.opencontainers.image.title="Doltgres" \
      org.opencontainers.image.description="Doltgres server runtime image" \
      org.opencontainers.image.source="${DOLTGRES_IMAGE_SOURCE}" \
      org.opencontainers.image.revision="${DOLTGRES_IMAGE_REVISION}" \
      org.opencontainers.image.version="${DOLTGRES_IMAGE_VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN mkdir /docker-entrypoint-initdb.d && \
    mkdir -p /var/lib/doltgres && \
    chmod 755 /var/lib/doltgres

COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

VOLUME /var/lib/doltgres

EXPOSE 5432 
WORKDIR /var/lib/doltgres
ENTRYPOINT ["tini", "-v", "--", "docker-entrypoint.sh"]

FROM runtime-base AS runtime-release
COPY --from=download-binary /usr/local/bin/doltgres* /usr/local/bin/
RUN /usr/local/bin/doltgres --version

FROM runtime-base AS runtime-source
COPY --from=build-from-source /usr/local/bin/doltgres /usr/local/bin/doltgres
RUN /usr/local/bin/doltgres --version

FROM runtime-release AS runtime
