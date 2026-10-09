FROM docker:latest AS docker-cli
FROM rust:latest AS rust

FROM ubuntu:26.04

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

COPY --from=rust /usr/local/rustup /usr/local/rustup
COPY --from=rust /usr/local/cargo /usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH="/usr/local/cargo/bin:${PATH}"

ENV GO_VERSION=1.27.2
ENV PATH=/usr/local/go/bin:${PATH}

ADD https://apt.llvm.org/llvm-snapshot.gpg.key /etc/apt/trusted.gpg.d/llvm.asc
ADD https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key /etc/apt/trusted.gpg.d/nodesource.asc
ADD https://www.postgresql.org/media/keys/ACCC4CF8.asc /etc/apt/trusted.gpg.d/pgdg.asc

RUN \
  chmod 0644 /etc/apt/trusted.gpg.d/llvm.asc \
             /etc/apt/trusted.gpg.d/nodesource.asc \
             /etc/apt/trusted.gpg.d/pgdg.asc && \
  apt-get update && \
  apt-get install -y --no-install-recommends ca-certificates && \
  echo "deb http://apt.llvm.org/resolute/ llvm-toolchain-resolute-23 main" > /etc/apt/sources.list.d/llvm-23.list && \
  echo "deb https://deb.nodesource.com/node_24.x nodistro main" > /etc/apt/sources.list.d/nodesource.list && \
  echo "deb https://apt.postgresql.org/pub/repos/apt resolute-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
  apt-get update && \
  apt-get install -y --no-install-recommends \
      curl wget gnupg \
      ninja-build cmake ccache make \
      llvm-23 clang-23 lld-23 libclang-rt-23-dev \
      bison flex libfl-dev \
      dh-make fakeroot \
      python3 python3-dev python3-pip perl \
      git gcc g++ binutils coreutils bash \
      postgresql-client-18 \
      systemd \
      nodejs \
      openjdk-25-jdk-headless maven \
      php-cli php-pgsql php-mbstring php-xml php-zip composer \
      dotnet-sdk-10.0 \
      libpq-dev pkg-config \
      ruby-full \
      r-base-core r-cran-dbi r-cran-yaml r-cran-bit64 r-cran-blob r-cran-hms r-cran-lubridate \
      r-cran-withr r-cran-cpp11 r-cran-plogr \
      sqlsmith && \
  ARCH=$(uname -m) && \
  case "$ARCH" in \
    x86_64)  GOARCH=amd64 ;; \
    aarch64) GOARCH=arm64 ;; \
    *) echo "unsupported arch $ARCH" && exit 1 ;; \
  esac && \
  curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${GOARCH}.tar.gz" \
    | tar -C /usr/local -xz && \
  Rscript -e 'install.packages("RPostgres", repos="https://cloud.r-project.org", quiet=TRUE)' && \
  Rscript -e 'library(RPostgres)' && \
  gem install --no-document pg && \
  ln -sf /usr/bin/clang-23 /usr/bin/clang && \
  ln -sf /usr/bin/clang++-23 /usr/bin/clang++ && \
  ln -sf /usr/bin/ccache /usr/local/bin/clang && \
  ln -sf /usr/bin/ccache /usr/local/bin/clang++ && \
  ln -sf /usr/bin/clang++-23 /usr/local/bin/g++ && \
  echo "UTC" > /etc/timezone

ENV CCACHE_DIR=/.ccache

ENV SDB_DRIVERS_DEPS=/opt/sdb-drivers
ENV GOMODCACHE=/opt/sdb-drivers/go/mod
ENV NUGET_PACKAGES=/opt/sdb-drivers/nuget
ENV GOTOOLCHAIN=local

COPY --from=drivers python/requirements.txt /tmp/drivers/python/
RUN python3 -m pip install --break-system-packages --no-cache-dir -r /tmp/drivers/python/requirements.txt

COPY --from=drivers js/package.json js/package-lock.json /opt/sdb-drivers/js/
RUN cd /opt/sdb-drivers/js && npm ci --no-fund --no-audit

COPY --from=drivers php/composer.json php/composer.lock /opt/sdb-drivers/php/
RUN cd /opt/sdb-drivers/php && composer install --no-interaction --no-progress

COPY --from=drivers go/go.mod go/go.sum /tmp/drivers/go/
RUN cd /tmp/drivers/go && go mod download && \
    GOBIN=/usr/local/bin go install github.com/jstemmer/go-junit-report/v2@v2.1.0

COPY --from=drivers java /tmp/drivers/java
COPY --from=drivers spec /tmp/drivers/spec
RUN cd /tmp/drivers/java && rm -rf target && \
    mvn -B -q -Dmaven.repo.local=/opt/sdb-drivers/m2 -Dmaven.test.failure.ignore=true test

COPY --from=drivers csharp/SerenedbDrivers.csproj /tmp/drivers/csharp/
RUN cd /tmp/drivers/csharp && dotnet restore

COPY --from=drivers rust /tmp/drivers/rust
RUN cargo fetch --locked --manifest-path /tmp/drivers/rust/Cargo.toml

RUN rm -rf /tmp/drivers && \
    chmod -R a+rwX /opt/sdb-drivers /usr/local/cargo

ENV PIP_NO_INDEX=1
ENV npm_config_offline=true
ENV COMPOSER_DISABLE_NETWORK=1
ENV GOPROXY=off
ENV GOFLAGS=-mod=readonly

CMD ["bash"]
