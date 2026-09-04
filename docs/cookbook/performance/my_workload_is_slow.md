---
layout: docu
redirect_from:
- /docs/guides/performance/my-workload-is-slow
- /docs/guides/performance/my_workload_is_slow
- /docs/preview/guides/performance/my_workload_is_slow
- /docs/stable/guides/performance/my_workload_is_slow
title: My Workload Is Slow
---

If you find that your workload in SereneDB is slow, we recommend performing the following checks. More detailed instructions are linked for each point.

1. Do you have enough memory? SereneDB works best if you have [1-4 GB memory per thread](./environment.md#memory).
1. Is your system maybe overcommitting memory, forcing the operating system to swap? Try _lowering_ the amount of memory available from the default 80% of the total RAM using `SET memory_limit = '...';`. While this sounds counter-intuitive, it can sometimes improve query performance, especially in memory-constrained environments where other processes are likely using more than 20% of the total system memory.
1. Are you using a fast disk? Network-attached disks (such as cloud block storage) cause write-intensive and [larger than memory](./how_to_tune_workloads.md#spilling-to-disk) workloads to slow down. For running such workloads in cloud environments, it is recommended to use instance-attached storage (NVMe SSDs).
1. Are you using indexes or constraints (primary key, unique, etc.)? If possible, try [disabling them](./schema.md#constraints), which boosts load and update performance.
1. Are you using the correct types? For example, [use `TIMESTAMP` to encode datetime values](./schema.md#types).
1. Are you reading from Parquet files? If so, do they have [row group sizes between 100k and 1M](./file_formats.md#the-effect-of-row-group-sizes) and file sizes between 100 MB to 10 GB?
1. Does the query plan look right? Study it with [`EXPLAIN`](./how_to_tune_workloads.md#profiling).
1. Is the workload running [in parallel](./how_to_tune_workloads.md#parallelism-multi-core-processing)? Use `htop` or the operating system's task manager to observe this.
1. Is SereneDB using too many threads? Try [limiting the amount of threads](./how_to_tune_workloads.md#parallelism-multi-core-processing).

Are you aware of other common issues? If so, please click the _Report content issue_ link below and describe them along with their workarounds.
