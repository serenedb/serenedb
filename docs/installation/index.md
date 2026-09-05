---
title: Installation
---

import DocCallout from "@site/src/components/DocCallout";

# Installation

SereneDB ships prebuilt for 64-bit Linux. Pick the method that fits where you are running it, then follow that page to install, start the server and connect.

## Choose an install method

| Method | Best for | Guide |
|---|---|---|
| **Shell script** | The fastest way to get a single host or your laptop running. Installs the `serened` binary into your home directory, no root required. | [Shell script](./shell-script.md) |
| **Docker** | Containers, CI and trying SereneDB on macOS or Windows. | [Docker](./docker.md) |
| **`.deb` package** | Debian and Ubuntu servers that should run SereneDB as a systemd-managed service. | [Debian / Ubuntu](./debian.md) |
| **Tarball** | Air-gapped hosts, custom install paths or when you manage the service yourself. | [Tarball](./tarball.md) |

<DocCallout type="tip">
New here? The [shell script](./shell-script.md) is the quickest path, then jump straight to the [Quick Start](../quick-start.md).
</DocCallout>

---

## System requirements

### CPU

Release binaries are statically linked and built for a baseline architecture:

| Architecture | Requirement |
|---|---|
| **x86-64** | Intel Haswell or newer — SSE 4.2 and AVX2 are required |
| **ARM64** | ARMv8-A (AArch64) |

On an older x86-64 CPU without AVX2 (pre-Haswell) the binary fails to start with an illegal-instruction error. More cores mean faster search and analytics — SereneDB parallelizes queries across the available CPUs.

### Memory

| | |
|---|---|
| Minimum | 2 GB |
| Recommended | 8 GB or more |

Memory use scales with your dataset size, index size and query concurrency. Full-text and analytical workloads benefit from extra RAM for caching.

### Storage

| Requirement | Details |
|---|---|
| Type | SSD strongly recommended |
| Filesystem | ext4 or xfs |

### Operating system

| OS | Support |
|---|---|
| **Linux** | Fully supported on any modern 64-bit distribution. Release binaries are statically linked, so there is no system library version to match. |
| **macOS** | No native binary yet — run it with [Docker](./docker.md). |
| **Windows** | No native binary yet — run it with [Docker](./docker.md). |

### Open file limit

SereneDB keeps many data and index files open at once, so the process needs a high open-file limit. The `.deb` service sets this for you (`LimitNOFILE=131072`). For a [shell-script](./shell-script.md) or [tarball](./tarball.md) install, raise it yourself before starting the server:

```sh
ulimit -n 131072
```

### Networking

SereneDB speaks the **PostgreSQL wire protocol** on port **7890** by default. A single-node deployment needs no other ports.

By default the server binds to **`127.0.0.1`** and only accepts connections from the same host. To accept remote connections you change the endpoint to `0.0.0.0` — see the [Debian / Ubuntu](./debian.md#connect-from-another-machine) guide or [Configuration](../configuration/overview.md). The [Docker](./docker.md) image already binds `0.0.0.0` inside the container.

---

For all server options see [Configuration](../configuration/overview.md).
