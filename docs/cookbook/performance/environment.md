---
layout: docu
redirect_from:
- /docs/guides/performance/environment
- /docs/preview/guides/performance/environment
- /docs/stable/guides/performance/environment
title: Environment
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The environment where SereneDB is run has an obvious impact on performance. This page focuses on the effects of the hardware configuration and the operating system used.

## Hardware Configuration

### CPU

SereneDB's officially supported architectures are AMD64 (x86_64) and ARM64 (AArch64) CPU architectures. SereneDB works efficiently on both of these architectures.

### Memory

<DocCallout type="bestPractice">

Aim for 1-4 GB memory per thread.

</DocCallout>

#### Minimum Required Memory

As a rule of thumb, SereneDB requires a _minimum_ of 125 MB of memory per thread.
For example, if you use 8 threads, you need at least 1 GB of memory.
If you are working in a memory-constrained environment, consider [limiting the number of threads](../../configuration/pragmas.md#threads), e.g., by issuing:

<SqlLogicTest id="cookbook/performance/environment/example_001" />

#### Memory for Ideal Performance

The amount of memory required for ideal performance depends on several factors, including the dataset size and the queries to execute.
Maybe surprisingly, the _queries_ have a larger effect on the memory requirement.
Workloads containing large joins over many-to-many tables yield large intermediate datasets and thus require more memory for their evaluation to fully fit into the memory.
As an approximation, aggregation-heavy workloads require 1-2 GB memory per thread and join-heavy workloads require 3-4 GB memory per thread.

#### Larger-than-Memory Workloads

SereneDB can process larger-than-memory workloads by spilling to disk.
This is possible thanks to _out-of-core_ support for grouping, joining, sorting and windowing operators.
Note that larger-than-memory workloads can be processed both in persistent mode and in in-memory mode as SereneDB still spills to disk in both modes.

### Local Disk

**Disk type.**
SereneDB's disk-based mode is designed to work best with SSD and NVMe disks. While HDDs are supported, they will result in low performance, especially for write operations.

**Disk-based vs. in-memory storage.**
Counter-intuitively, using a disk-based SereneDB instance can be faster than an in-memory instance.
This is because the default settings prescribe compression for disk-based storage but they do turn compression off for in-memory storage.
Read more in the [“How to Tune Workloads” page](./how_to_tune_workloads.md#persistent-vs-in-memory-tables).

**File systems.**
On Linux, [SereneDB performs best with the XFS file system](https://www.phoronix.com/review/linux-70-filesystems/4) but it also performs well with other file systems such as ext4.
On Windows, we recommend using NTFS and avoiding FAT32.

<DocCallout type="tip">

SereneDB databases have built-in checksums, so integrity checks from the file system are not required to prevent data corruption.

</DocCallout>

### Network-Attached Disks

Special care needs to be taken when using network-attached disks:

* If you are writing to disk, it is important that the disks are reliable. As a general rule of thumb, this is true for locally attached disks, and block storage in the cloud.
* If your workload is larger than memory and/or fast data loading is important, you need fast disks, preferably SSD or NVMe with a fast connection.

With these in mind, here are two common architectures and the related considerations when you are using SereneDB's native database format:

**Block storage in the cloud.** SereneDB runs well on network-backed cloud disks such as [AWS EBS](https://aws.amazon.com/ebs/) for both read-only and read-write workloads.

**Network-attached storage.**
Network-attached storage can serve SereneDB for read-only workloads.
However, _it is recommended to avoid using SereneDB's native database format in read-write mode on network-attached storage (NAS)._
These setups include [NFS](https://en.wikipedia.org/wiki/Network_File_System),
network drives such as [SMB](https://en.wikipedia.org/wiki/Server_Message_Block) and
[Samba](https://en.wikipedia.org/wiki/Samba_(software)).
Based on user reports, running read-write workloads on network-attached storage can result in slow and unpredictable performance,
as well as spurious errors caused by the underlying file system.
Instead of using SereneDB's native database format, consider using the [DuckLake lakehouse format](https://ducklake.select/).

## Operating System

We recommend using the latest stable version of operating systems: macOS, Windows, and Linux are all well-tested and SereneDB can run on them with high performance.

### Linux

SereneDB runs on all mainstream Linux distributions released in the last ≈5 years.
If you don't have a particular preference, we recommend using Ubuntu Linux LTS due to its stability and the fact that most of SereneDB’s Linux test suite jobs run on Ubuntu workers.

#### glibc vs. musl libc

We distribute SereneDB builds with both [glibc](https://www.gnu.org/software/libc/) and [musl libc](https://www.musl-libc.org/).
However, note that SereneDB binaries built with musl libc have significantly lower performance.
In practice, this can lead to a slowdown of more than 5× on compute-intensive workloads.
Therefore, it's recommended to use a Linux distribution with glibc for performance-oriented workloads when running SereneDB.

#### File Descriptors and Memory Maps

SereneDB keeps many files open at once — the database file, the write-ahead log, external Parquet, CSV and JSON sources and, above all, the per-segment files of [inverted indexes](../../sql/indexes/inverted/index.md) — and it memory-maps those index segments. Two Linux limits therefore matter for search-heavy and large workloads.

**Open files (`RLIMIT_NOFILE`).**
On startup SereneDB raises its own _soft_ open-file limit to 65535, or to the _hard_ limit if that is lower. Raising the hard limit requires elevated privileges, so set it before starting the server if it sits below that. The `.deb` package's service does this for you (`LimitNOFILE=131072`); for a [shell-script](../../installation/shell-script.md) or [tarball](../../installation/tarball.md) install, raise it yourself first:

```sh
ulimit -n 131072
```

A limit that is too low surfaces as `Too many open files` errors under load. See the [installation notes](../../installation/index.md) for more.

**Memory maps (`vm.max_map_count`).**
Inverted indexes are backed by [IResearch](https://github.com/serenedb/serenedb/tree/main/libs/iresearch), which memory-maps every index segment. A workload that produces many segments can exhaust the kernel's default cap on memory-map areas (65530 on most distributions), and further `mmap` calls then fail. SereneDB checks this value at startup and warns when it is below the recommended minimum of **262144**. Raise it with:

```sh
sudo sysctl -w vm.max_map_count=262144
```

To persist the setting across reboots, add `vm.max_map_count=262144` to `/etc/sysctl.conf` or a file under `/etc/sysctl.d/`.

## Memory Allocator

If you have a many-core CPU running on a system where SereneDB ships with `jemalloc` as the default memory allocator, consider enabling the allocator's background threads.
