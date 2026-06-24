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

#include "basics/number_of_cores.h"

#include <absl/strings/str_split.h>
#include <absl/strings/string_view.h>
#include <fast_float/fast_float.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "basics/operating-system.h"

namespace {

template<typename T>
std::optional<T> ParseNumber(std::string_view sv) {
  sv = absl::StripAsciiWhitespace(sv);
  T value;
  const auto res =
    fast_float::from_chars(sv.data(), sv.data() + sv.size(), value);
  if (res.ec != std::errc{}) {
    return std::nullopt;
  }
  return value;
}

int64_t OnlineCores() {
#ifdef SERENEDB_SC_NPROCESSORS_ONLN
  const int64_t n = sysconf(_SC_NPROCESSORS_ONLN);
  if (n > 0) {
    return n;
  }
#endif
  const unsigned hc = std::thread::hardware_concurrency();
  return hc > 0 ? static_cast<int64_t>(hc) : int64_t{1};
}

// CFS CPU quota as whole cores, rounded up (1.5 cores -> 2);
// 0 = no quota. cgroup v2 unified first, then v1.
int64_t CfsQuotaCores() {
  // v2: /sys/fs/cgroup/cpu.max = "<quota> <period>" | "max <period>".
  {
    std::ifstream f("/sys/fs/cgroup/cpu.max");
    std::string line;
    if (std::getline(f, line)) {
      const std::vector<std::string_view> parts = absl::StrSplit(
        absl::StripAsciiWhitespace(line), ' ', absl::SkipEmpty());
      if (parts.size() == 2 && parts[0] != "max") {
        const auto period = ParseNumber<int64_t>(parts[1]);
        const auto quota = ParseNumber<int64_t>(parts[0]);
        if (period && *period > 0 && quota && *quota > 0) {
          return static_cast<int64_t>(std::ceil(static_cast<double>(*quota) /
                                                static_cast<double>(*period)));
        }
      }
    }
  }
  // v1: cpu.cfs_quota_us / cpu.cfs_period_us (quota -1 => unlimited).
  for (const char* dir : {"/sys/fs/cgroup/cpu", "/sys/fs/cgroup/cpu,cpuacct"}) {
    int64_t quota = -1;
    int64_t period = 0;
    std::ifstream qf(std::string{dir} + "/cpu.cfs_quota_us");
    std::ifstream pf(std::string{dir} + "/cpu.cfs_period_us");
    if (qf >> quota && pf >> period && quota > 0 && period > 0) {
      return static_cast<int64_t>(
        std::ceil(static_cast<double>(quota) / static_cast<double>(period)));
    }
  }
  return 0;
}

// Parse a kernel "0-3,7" cpu list into the set of logical cpu ids.
sdb::containers::FlatHashSet<int64_t> ParseCpuList(std::string_view list) {
  sdb::containers::FlatHashSet<int64_t> cpus;
  for (const std::string_view part : absl::StrSplit(list, ',')) {
    const auto dash = part.find('-');
    if (dash == std::string_view::npos) {
      if (const auto cpu = ParseNumber<int64_t>(part)) {
        cpus.insert(*cpu);
      }
    } else {
      const auto lo = ParseNumber<int64_t>(part.substr(0, dash));
      const auto hi = ParseNumber<int64_t>(part.substr(dash + 1));
      if (lo && hi) {
        for (int64_t c = *lo; c <= *hi; ++c) {
          cpus.insert(c);
        }
      }
    }
  }
  return cpus;
}

// Count CPUs in a kernel "0-3,7" cpu list; 0 if empty/malformed.
int64_t CountCpuList(std::string_view list) {
  return static_cast<int64_t>(ParseCpuList(list).size());
}

// Count CPUs in a "0-3,7" cpuset list; 0 = no cpuset. cgroup v2 then v1.
int64_t CpusetCores() {
  const auto parse = [](const char* path) -> int64_t {
    std::ifstream f(path);
    std::string list;
    if (!std::getline(f, list) || list.empty()) {
      return 0;
    }
    return CountCpuList(list);
  };
  if (const int64_t v2 = parse("/sys/fs/cgroup/cpuset.cpus.effective");
      v2 > 0) {
    return v2;
  }
  return parse("/sys/fs/cgroup/cpuset/cpuset.cpus");
}

// The cgroup-allowed logical cpu set (cpuset.cpus.effective), cgroup v2 then
// v1; empty = no cpuset restriction (any cpu allowed).
sdb::containers::FlatHashSet<int64_t> AllowedCpuSet() {
  for (const char* path : {"/sys/fs/cgroup/cpuset.cpus.effective",
                           "/sys/fs/cgroup/cpuset/cpuset.cpus"}) {
    std::ifstream f(path);
    std::string list;
    if (std::getline(f, list) && !list.empty()) {
      if (auto cpus = ParseCpuList(list); !cpus.empty()) {
        return cpus;
      }
    }
  }
  return {};
}

// Map each logical cpu to its physical core (physical_id, core_id) from
// /proc/cpuinfo; empty when topology is unavailable (chroot / some VMs).
sdb::containers::FlatHashMap<int64_t, std::pair<int64_t, int64_t>>
CpuCoreMap() {
  std::ifstream f("/proc/cpuinfo");
  sdb::containers::FlatHashMap<int64_t, std::pair<int64_t, int64_t>> map;
  int64_t cpu = -1;
  int64_t physical_id = 0;
  std::string line;
  while (std::getline(f, line)) {
    const std::string_view sv = line;
    const auto colon = sv.find(':');
    if (colon == std::string_view::npos) {
      continue;
    }
    const std::string_view key = sv.substr(0, colon);
    const std::string_view val = sv.substr(colon + 1);
    if (key.find("processor") != std::string_view::npos) {
      if (const auto v = ParseNumber<int64_t>(val)) {
        cpu = *v;
      }
    } else if (key.find("physical id") != std::string_view::npos) {
      if (const auto v = ParseNumber<int64_t>(val)) {
        physical_id = *v;
      }
    } else if (key.find("core id") != std::string_view::npos && cpu >= 0) {
      if (const auto v = ParseNumber<int64_t>(val)) {
        map[cpu] = {physical_id, *v};
      }
    }
  }
  return map;
}

int64_t NumberOfCoresImpl() {
  return sdb::CountLogicalCores(OnlineCores(), CfsQuotaCores(), CpusetCores());
}

}  // namespace
namespace sdb {

int64_t CountLogicalCores(int64_t online, int64_t cfs_quota_cores,
                          int64_t cpuset_cores) noexcept {
  int64_t cores = online != 0 ? online : int64_t{1};
  if (cfs_quota_cores != 0) {
    cores = std::min(cores, cfs_quota_cores);
  }
  if (cpuset_cores != 0) {
    cores = std::min(cores, cpuset_cores);
  }
  return cores != 0 ? cores : int64_t{1};
}

int64_t CountLogicalCores() {
  static const int64_t kCached = NumberOfCoresImpl();
  return kCached;
}

int64_t CountPhysicalCores(const containers::FlatHashMap<
                             int64_t, std::pair<int64_t, int64_t>>& cpu_to_core,
                           const containers::FlatHashSet<int64_t>& allowed,
                           int64_t cfs_quota_cores) noexcept {
  const int64_t host_logical = static_cast<int64_t>(cpu_to_core.size());
  containers::FlatHashSet<std::pair<int64_t, int64_t>> host;
  for (const auto& [cpu, core] : cpu_to_core) {
    host.insert(core);
  }
  const int64_t host_physical = static_cast<int64_t>(host.size());

  // cpuset dimension (concrete cores): distinct physical cores among the
  // allowed cpus, so a cpuset pinning HT sibling pairs collapses to the real
  // count.
  int64_t physical = host_physical;
  if (!allowed.empty()) {
    containers::FlatHashSet<std::pair<int64_t, int64_t>> in_set;
    for (const int64_t cpu : allowed) {
      if (const auto it = cpu_to_core.find(cpu); it != cpu_to_core.end()) {
        in_set.insert(it->second);
      }
    }
    // No topology for the allowed cpus -> assume no SMT (logical == physical).
    physical = !in_set.empty() ? static_cast<int64_t>(in_set.size())
                               : static_cast<int64_t>(allowed.size());
  }

  // CFS quota dimension (non-concrete): a quota is a fraction of the machine's
  // CPU time, not a core set, so apply the same fraction to the physical cores
  // (round up) rather than treating quota-cores as physical-cores.
  if (cfs_quota_cores != 0 && host_logical != 0) {
    const int64_t from_quota =
      (host_physical * cfs_quota_cores + host_logical - 1) / host_logical;
    physical = std::min(physical, from_quota);
  }

  return physical != 0 ? physical : int64_t{1};
}

int64_t CountPhysicalCores() {
  static const int64_t kCached = [] {
    const auto map = CpuCoreMap();
    if (map.empty()) {
      // No /proc/cpuinfo topology (chroot / some VMs): fall back to the
      // cgroup-aware logical count.
      return CountLogicalCores();
    }
    return CountPhysicalCores(map, AllowedCpuSet(), CfsQuotaCores());
  }();
  return kCached;
}

}  // namespace sdb
