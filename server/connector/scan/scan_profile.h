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

#include <atomic>
#include <cstdint>
#include <duckdb/common/time_point.hpp>
#include <duckdb/main/profiler/profiling_node.hpp>
#include <iresearch/search/fill/node.hpp>
#include <iresearch/search/filters/query_profile.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <iresearch/utils/memory.hpp>
#include <memory>
#include <type_traits>
#include <typeinfo>
#include <vector>

#include "connector/scan/scan_plan.h"
#include "connector/search_filter_printer.hpp"

namespace sdb::connector {

struct ShapeStats {
  uint64_t plans = 0;
  uint64_t calls = 0;
  uint64_t rows = 0;
  uint64_t plan_ticks = 0;
  uint64_t run_ticks = 0;
};

struct ScanLocalProfile {
  bool enabled = false;
  irs::containers::NodeHashMap<const std::type_info*, ShapeStats> shapes;
  ShapeStats* current = nullptr;
  uint64_t filtered = 0;
  uint64_t fetch_ticks = 0;
  uint64_t lookup_ticks = 0;
  uint64_t stats_ticks = 0;
};

inline uint64_t ElapsedTicks(int64_t start) noexcept {
  return static_cast<uint64_t>(duckdb::TimePoint::Now() - start);
}

class [[nodiscard]] ScanTimer {
 public:
  ScanTimer(ScanLocalProfile& profile,
            uint64_t ScanLocalProfile::* ticks) noexcept
    : _profile{profile},
      _ticks{ticks},
      _start{profile.enabled ? duckdb::TimePoint::Now() : 0} {}

  ScanTimer(const ScanTimer&) = delete;
  ScanTimer& operator=(const ScanTimer&) = delete;

  ~ScanTimer() {
    if (_profile.enabled) {
      _profile.*_ticks += ElapsedTicks(_start);
    }
  }

  void Filtered(uint64_t rows) noexcept {
    if (_profile.enabled) {
      _profile.filtered += rows;
    }
  }

 private:
  ScanLocalProfile& _profile;
  uint64_t ScanLocalProfile::* _ticks;
  int64_t _start;
};

template<typename Make>
auto Planned(ScanLocalProfile& profile, Make&& make) {
  if (!profile.enabled) [[likely]] {
    auto root = make();
    EnsurePlanned(root != nullptr);
    return root;
  }
  const auto start = duckdb::TimePoint::Now();
  auto root = make();
  const auto ticks = ElapsedTicks(start);
  EnsurePlanned(root != nullptr);
  auto& shape = profile.shapes[&typeid(*root)];
  ++shape.plans;
  shape.plan_ticks += ticks;
  profile.current = &shape;
  return root;
}

template<typename Run>
auto Ran(ScanLocalProfile& profile, Run&& run) {
  auto* shape = profile.current;
  if (!shape) [[likely]] {
    return run();
  }
  const auto start = duckdb::TimePoint::Now();
  if constexpr (std::is_void_v<std::invoke_result_t<Run>>) {
    run();
    shape->run_ticks += ElapsedTicks(start);
    ++shape->calls;
  } else {
    auto rows = run();
    shape->run_ticks += ElapsedTicks(start);
    ++shape->calls;
    shape->rows += rows;
    return rows;
  }
}

template<typename Run, typename Total>
void Ran(ScanLocalProfile& profile, Run&& run, Total&& total) {
  auto* shape = profile.current;
  if (!shape) [[likely]] {
    run();
    return;
  }
  const auto before = total();
  const auto start = duckdb::TimePoint::Now();
  run();
  shape->run_ticks += ElapsedTicks(start);
  ++shape->calls;
  shape->rows += total() - before;
}

class ScanProfile {
 public:
  ScanProfile(std::vector<irs::FilterBox> boxes, bool nested,
              uint64_t snapshot_ticks);

  irs::QueryProfile& Query() noexcept { return _query; }

  void SetSegments(uint64_t ticks, uint64_t kept, uint64_t total) noexcept {
    _classify_ticks = ticks;
    _segments_kept = kept;
    _segments_total = total;
  }

  void FinishInit(uint32_t workers);

  void Report(uint32_t worker, ScanLocalProfile local,
              duckdb::OperatorMetrics& metrics);

 private:
  struct Worker {
    ScanLocalProfile profile;
    std::atomic_bool published = false;
  };

  void Render(duckdb::ProfilingNode& node) const;

  std::vector<irs::FilterBox> _boxes;
  irs::QueryProfile _query;
  uint64_t _snapshot_ticks = 0;
  uint64_t _classify_ticks = 0;
  uint64_t _segments_kept = 0;
  uint64_t _segments_total = 0;
  std::atomic_uint64_t _init_ticks = 0;
  std::unique_ptr<Worker[]> _workers;
  uint32_t _workers_size = 0;
  std::atomic_uint64_t _version = 0;
};

irs::fill::Node::ptr ProfileFill(const ScanLocalProfile& profile,
                                 irs::fill::Node::ptr node);

}  // namespace sdb::connector
