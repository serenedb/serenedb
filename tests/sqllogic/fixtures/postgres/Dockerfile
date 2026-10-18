ARG POSTGRES_VERSION=18.6

FROM postgres:${POSTGRES_VERSION}

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-18-postgis-3 \
    && rm -rf /var/lib/apt/lists/*
