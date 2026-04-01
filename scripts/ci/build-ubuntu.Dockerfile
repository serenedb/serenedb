FROM docker:latest AS docker-cli
FROM rust:latest AS rust

FROM ubuntu:24.04

ARG TARGETARCH
ARG RCLONE_VERSION=v1.73.1

# Copy Docker CLI
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

# Copy Rust toolchain
COPY --from=rust /usr/local/rustup /usr/local/rustup
COPY --from=rust /usr/local/cargo /usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH="/usr/local/cargo/bin:${PATH}"

# Combined layer for dependencies, configuration, and cleanup
RUN \
  apt-get update && \
  # 1. Install setup tools
  apt-get install -y --no-install-recommends wget unzip gnupg ca-certificates && \
  \
  # 2. Setup LLVM Repository
  wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key | tee /etc/apt/trusted.gpg.d/apt.llvm.org.asc && \
  echo "deb http://apt.llvm.org/noble/ llvm-toolchain-noble-21 main" > /etc/apt/sources.list.d/llvm-toolchain-noble-21.list && \
  apt-get update && \
  \
  # 3. Install Build & Environment Dependencies
  apt-get install -y --no-install-recommends \
      bison flex libfl-dev make ninja-build cmake ccache git dh-make \
      llvm-21 clang-21 lld-21 libclang-rt-21-dev \
      perl python3 python3-dev python3-pip \
      binutils coreutils bash gdb openssh-client groff net-tools lcov postgresql-client && \
  \
  # 4. Install Python packages
  pip3 install --no-cache-dir --break-system-packages psutil pyyaml && \
  \
  # 5. Create symlinks
  ln -sf /usr/bin/clang-21 /usr/bin/clang && \
  ln -sf /usr/bin/clang++-21 /usr/bin/clang++ && \
  ln -sf /usr/bin/ccache /usr/local/bin/clang && \
  ln -sf /usr/bin/ccache /usr/local/bin/clang++ && \
  \
  # 6. Install rclone
  wget --no-check-certificate "https://github.com/rclone/rclone/releases/download/${RCLONE_VERSION}/rclone-${RCLONE_VERSION}-linux-${TARGETARCH}.zip" && \
  unzip -q "rclone-${RCLONE_VERSION}-linux-${TARGETARCH}.zip" && \
  mv "rclone-${RCLONE_VERSION}-linux-${TARGETARCH}/rclone" /usr/sbin/rclone && \
  rm -rf "rclone-${RCLONE_VERSION}-linux-${TARGETARCH}"* && \
  \
  # 7. System Configuration & Cleanup
  echo "UTC" > /etc/timezone && \
  apt-get autopurge -y wget unzip gnupg && \
  rm -rf /var/lib/apt/lists/*

ENV CCACHE_DIR=/.ccache

CMD [ "bash" ]
