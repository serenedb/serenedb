# ---- Extract Stage ----
FROM ubuntu:24.04 AS extractor

COPY install.tar.gz /tmp/
RUN mkdir -p /tmp/serenedb-extract \
    && tar -xzf /tmp/install.tar.gz -C /tmp/serenedb-extract --strip-components=1 \
    && cd /tmp/serenedb-extract \
    && if [ -d sbin ]; then cp -f sbin/* usr/sbin/; rm -rf sbin; fi \
    && if [ -d bin ]; then cp -f bin/* usr/bin/; rm -rf bin; fi

# ---- Runtime Stage ----
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

# Copy extracted files from build stage
COPY --from=extractor /tmp/serenedb-extract/ /

# Setup script (creates directories, patches config, installs rclone)
COPY setup.sh /tmp/setup.sh
RUN chmod +x /tmp/setup.sh \
    && /tmp/setup.sh \
    && rm -f /tmp/setup.sh

# Remove unnecessary packages and clean up
RUN apt-get update \
    && apt-get remove --purge -y \
        e2fsprogs \
        login \
        passwd \
        mount \
    ; apt-get autoremove -y \
    ; apt-get clean \
    # Delete GPG binaries directly (packages may not be installed)
    && rm -f \
        /usr/bin/gpg \
        /usr/bin/gpgv \
        /usr/bin/gpg-agent \
        /usr/bin/gpgconf \
        /usr/bin/gpgsm \
        /usr/bin/dirmngr \
    # Delete caches, docs, logs, locales
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/apt/* \
        /var/log/* \
        /usr/share/doc/* \
        /usr/share/man/* \
        /usr/share/info/* \
        /usr/share/locale/* \
        /usr/share/i18n/* \
        /usr/share/lintian/* \
        /usr/share/bug/* \
        /usr/share/zsh/* \
        /tmp/* \
        /var/tmp/* \
        /root/.cache \
        /var/lib/dpkg/info/*.md5sums

# Environment
ENV GLIBCXX_FORCE_NEW=1

# Default timezone
RUN echo "UTC" > /etc/timezone

# Permissions for OpenShift compatibility
RUN chgrp -R 0 /var/lib/serenedb \
    && chmod -R 775 /var/lib/serenedb

# Volumes
VOLUME ["/var/lib/serenedb"]

# Entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8529

ENTRYPOINT ["/entrypoint.sh"]
CMD ["serened"]
