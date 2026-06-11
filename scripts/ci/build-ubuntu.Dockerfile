FROM docker:latest AS docker-cli
FROM rust:latest AS rust

FROM ubuntu:24.04

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

COPY --from=rust /usr/local/rustup /usr/local/rustup
COPY --from=rust /usr/local/cargo /usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH="/usr/local/cargo/bin:${PATH}"

ENV GO_VERSION=1.22.10
ENV PATH=/usr/local/go/bin:${PATH}

ADD https://apt.llvm.org/llvm-snapshot.gpg.key /etc/apt/trusted.gpg.d/llvm.asc
ADD https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key /etc/apt/trusted.gpg.d/nodesource.asc
ADD https://packages.microsoft.com/keys/microsoft.asc /etc/apt/trusted.gpg.d/microsoft.asc
ADD https://www.postgresql.org/media/keys/ACCC4CF8.asc /etc/apt/trusted.gpg.d/pgdg.asc

RUN \
  chmod 0644 /etc/apt/trusted.gpg.d/llvm.asc \
             /etc/apt/trusted.gpg.d/nodesource.asc \
             /etc/apt/trusted.gpg.d/microsoft.asc \
             /etc/apt/trusted.gpg.d/pgdg.asc && \
  apt-get update && \
  apt-get install -y --no-install-recommends ca-certificates && \
  echo "deb http://apt.llvm.org/noble/ llvm-toolchain-noble-21 main" > /etc/apt/sources.list.d/llvm-21.list && \
  echo "deb https://deb.nodesource.com/node_20.x nodistro main" > /etc/apt/sources.list.d/nodesource.list && \
  echo "deb https://packages.microsoft.com/ubuntu/24.04/prod noble main" > /etc/apt/sources.list.d/microsoft-prod.list && \
  echo "deb https://apt.postgresql.org/pub/repos/apt noble-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
  apt-get update && \
  apt-get install -y --no-install-recommends \
      curl wget gnupg \
      ninja-build cmake ccache make \
      llvm-21 clang-21 lld-21 libclang-rt-21-dev \
      bison flex libfl-dev \
      dh-make fakeroot \
      python3 python3-dev python3-pip perl \
      git gcc binutils coreutils bash \
      postgresql-client-18 \
      systemd \
      nodejs \
      openjdk-21-jdk-headless maven \
      php-cli php-pgsql php-mbstring php-xml php-zip composer \
      dotnet-sdk-8.0 \
      libpq-dev pkg-config \
      ruby-full \
      r-base-core \
      sqlsmith && \
  ARCH=$(uname -m) && \
  case "$ARCH" in \
    x86_64)  GOARCH=amd64 ;; \
    aarch64) GOARCH=arm64 ;; \
    *) echo "unsupported arch $ARCH" && exit 1 ;; \
  esac && \
  curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${GOARCH}.tar.gz" \
    | tar -C /usr/local -xz && \
  python3 -m pip install --break-system-packages --no-cache-dir \
      pytest pyyaml \
      "psycopg[binary]" psycopg2-binary asyncpg pytest-asyncio && \
  ln -sf /usr/bin/clang-21 /usr/bin/clang && \
  ln -sf /usr/bin/clang++-21 /usr/bin/clang++ && \
  ln -sf /usr/bin/ccache /usr/local/bin/clang && \
  ln -sf /usr/bin/ccache /usr/local/bin/clang++ && \
  ln -sf /usr/bin/clang++-21 /usr/local/bin/g++ && \
  echo "UTC" > /etc/timezone

ENV CCACHE_DIR=/.ccache

CMD ["bash"]
