////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <utility>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"

namespace sdb {

/// Number of CPU cores available to this process: the host's online CPUs
/// clamped by any cgroup CFS quota and cpuset (so a Docker `--cpus=N` /
/// `--cpuset-cpus` limit is respected, not the host's full core count).
int64_t CountLogicalCores();

/// Combine the host's online CPU count with container limits: a CFS CPU quota
/// in whole cores (rounded up; 0 = no quota) and a cpuset size (0 = none). The
/// result is the most restrictive of the three, clamped to >= 1. Pure +
/// testable; CountLogicalCores() feeds it values read from /sys/fs/cgroup.
int64_t CountLogicalCores(int64_t online, int64_t cfs_quota_cores,
                          int64_t cpuset_cores) noexcept;

/// Physical CPU cores available to this process: the distinct physical cores
/// among the cgroup-allowed cpuset (cpuset.cpus.effective), capped by any CFS
/// quota. With no cgroup it's the host's physical core count (HT siblings
/// dropped); a cpuset pinning sibling pairs collapses to real physical cores.
/// For pools that gain nothing from hyper-threads -- e.g. epoll io reactors.
int64_t CountPhysicalCores();

/// Pure: distinct physical cores (physical_id, core_id) among `allowed` logical
/// cpus (all of `cpu_to_core` when `allowed` is empty), capped by
/// cfs_quota_cores (0 = none), clamped to >= 1. Testable.
int64_t CountPhysicalCores(const containers::FlatHashMap<
                             int64_t, std::pair<int64_t, int64_t>>& cpu_to_core,
                           const containers::FlatHashSet<int64_t>& allowed,
                           int64_t cfs_quota_cores) noexcept;

}  // namespace sdb
