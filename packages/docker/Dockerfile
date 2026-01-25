# =============================================================================
# SereneDB Docker Image
# =============================================================================
FROM ubuntu:24.04

LABEL maintainer="SereneDB"
LABEL org.opencontainers.image.title="SereneDB"
LABEL org.opencontainers.image.description="SereneDB Database Server"
LABEL org.opencontainers.image.vendor="SereneDB"

# Install runtime dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        numactl \
        pwgen \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy tarball, extract with strip to temp location, then merge to root
COPY install.tar.gz /tmp/
RUN mkdir -p /tmp/serenedb-extract \
    && tar -xzf /tmp/install.tar.gz -C /tmp/serenedb-extract --strip-components=1 \
    && cd /tmp/serenedb-extract \
    && if [ -d sbin ]; then cp -f sbin/* usr/sbin/; rm -rf sbin; fi \
    && if [ -d bin ]; then cp -f bin/* usr/bin/; rm -rf bin; fi \
    && cp -a . / \
    && rm -rf /tmp/*

# Setup script (creates directories, patches config, installs rclone)
COPY setup.sh /tmp/setup.sh
RUN chmod +x /tmp/setup.sh \
    && /tmp/setup.sh \
    && rm -f /tmp/setup.sh

# Environment
ENV GLIBCXX_FORCE_NEW=1
ENV SERENE_INIT_PORT=8999

# Default timezone
RUN echo "UTC" > /etc/timezone

# Permissions for OpenShift compatibility
# OpenShift runs containers with random UID and GID 0
RUN chgrp -R 0 /var/lib/serenedb \
    && chmod -R 775 /var/lib/serenedb \
    && chgrp -R 0 /var/log/serenedb \
    && chmod -R 775 /var/log/serenedb

# Volumes
VOLUME ["/var/lib/serenedb", "/var/log/serenedb"]

# Entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8529

ENTRYPOINT ["/entrypoint.sh"]
CMD ["serened"]
