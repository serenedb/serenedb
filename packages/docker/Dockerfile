FROM ubuntu:24.04

LABEL maintainer="SereneDB GmbH <support@serenedb.com>"
LABEL org.opencontainers.image.title="SereneDB"
LABEL org.opencontainers.image.description="SereneDB Database Server"
LABEL org.opencontainers.image.vendor="SereneDB GmbH"

# Extract pre-built binaries into filesystem
ARG TARGETARCH
COPY install-${TARGETARCH}.tar.gz /tmp/install.tar.gz

# All setup in single layer to minimize image size
RUN \
  # Extract pre-built binaries (strip top-level dir name from tarball)
  tar -xzf /tmp/install.tar.gz -C / --strip-components=1 && \
  rm /tmp/install.tar.gz && \
  # numactl: NUMA optimization for multi-socket systems
  # postgresql-client: psql for debugging, pg_isready for health checks
  apt-get update && \
  apt-get install -y --no-install-recommends numactl postgresql-client && \
  # Create non-root user for running serened
  groupadd -r serenedb && \
  useradd -r -g serenedb -d /var/lib/serenedb -s /usr/sbin/nologin serenedb && \
  # Create data directory (775 + group 0 for OpenShift compatibility)
  install -o serenedb -g 0 -m 775 -d /var/lib/serenedb && \
  # TODO: add /docker-entrypoint-initdb.d for PostgreSQL-style init scripts (.sh/.sql)
  # Bind to all interfaces and log to stdout (for docker networking and journald)
  if [ -f /etc/serenedb/serened.conf ]; then \
    sed -i -e 's~^--listen=.*~--listen=postgres://0.0.0.0:7890~' /etc/serenedb/serened.conf; \
    sed -i -e 's~^--log_storage=.*~--log_storage=stdout~' /etc/serenedb/serened.conf; \
  fi && \
  # Explicitly set UTC timezone (ubuntu:24.04 defaults to UTC, but be safe)
  echo "UTC" > /etc/timezone && \
  # Cleanup: remove unused base packages, apt cache, docs, locales
  apt-get autopurge -y --allow-remove-essential e2fsprogs login passwd mount && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /var/cache/apt/* /var/log/* \
         /usr/share/doc/* /usr/share/man/* /usr/share/info/* \
         /usr/share/locale/* /usr/share/i18n/* /root/.cache /tmp/*

VOLUME ["/var/lib/serenedb"]
EXPOSE 7890

COPY entrypoint.sh /entrypoint.sh

USER serenedb
ENTRYPOINT ["/entrypoint.sh"]
CMD ["serened"]
