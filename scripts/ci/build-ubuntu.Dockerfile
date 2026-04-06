FROM docker:latest AS docker-cli
FROM rust:latest AS rust

FROM ubuntu:24.04

# Copy Docker CLI (for sqllogic tests that start MinIO)
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

# Copy Rust toolchain (for building sqllogictest-rs)
COPY --from=rust /usr/local/rustup /usr/local/rustup
COPY --from=rust /usr/local/cargo /usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH="/usr/local/cargo/bin:${PATH}"

# All dependencies in single layer to minimize image size
RUN apt-get update && \
  # Setup LLVM repo (ca-certificates needed for HTTPS)
  apt-get install -y --no-install-recommends wget ca-certificates && \
  wget -qO /etc/apt/trusted.gpg.d/llvm.asc https://apt.llvm.org/llvm-snapshot.gpg.key && \
  echo "deb http://apt.llvm.org/noble/ llvm-toolchain-noble-21 main" > /etc/apt/sources.list.d/llvm-21.list && \
  apt-get update && \
  apt-get install -y --no-install-recommends \
      # Build system
      ninja-build cmake ccache make \
      # C++ toolchain (LLVM 21)
      llvm-21 clang-21 lld-21 libclang-rt-21-dev \
      # Parser generators (for grammar files)
      bison flex libfl-dev \
      # Debian packaging
      dh-make fakeroot \
      # Scripting (build scripts, code generation)
      python3 python3-dev python3-pip perl \
      # Core utilities (gcc provides /usr/bin/cc needed by cargo's cc crate)
      git gcc binutils coreutils bash \
      # Testing: pg_isready/psql for sqllogic tests
      postgresql-client \
      # Testing: systemd for deb RTA (service lifecycle tests)
      systemd && \
  # Compiler symlinks (ccache wraps clang via /usr/local/bin)
  ln -sf /usr/bin/clang-21 /usr/bin/clang && \
  ln -sf /usr/bin/clang++-21 /usr/bin/clang++ && \
  ln -sf /usr/bin/ccache /usr/local/bin/clang && \
  ln -sf /usr/bin/ccache /usr/local/bin/clang++ && \
  # Explicitly set UTC timezone (ubuntu:24.04 defaults to UTC, but be safe)
  echo "UTC" > /etc/timezone && \
  # Cleanup temporary packages
  apt-get autopurge -y wget && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV CCACHE_DIR=/.ccache

CMD ["bash"]
