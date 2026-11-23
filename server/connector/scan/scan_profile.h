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
#include <duckdb/common/explain_value.hpp>
#include <duckdb/common/time_point.hpp>
#include <duckdb/main/profiler/profiling_node.hpp>
#include <iresearch/index/index_source.hpp>
#include <iresearch/search/filters/query_profile.hpp>
#include <memory>
#include <string>
#include <vector>

#include "connector/search_filter_printer.hpp"

namespace sdb::connector {

inline uint64_t ElapsedTicks(int64_t start) noexcept {
  return static_cast<uint64_t>(duckdb::TimePoint::Now() - start);
}

struct ScanChecks {
  std::vector<duckdb::ExplainNode> nodes;
  std::vector<duckdb::ExplainNode> duals;
  bool duals_on_rows = false;
};

class ScanProfile {
 public:
  using Info = duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>;

  ScanProfile(irs::FilterTree filter, ScanChecks checks,
              std::string lookup_source, bool nested, uint64_t snapshot_ticks);

  irs::QueryProfile& Query() noexcept { return _query; }

  void SetSegments(uint64_t ticks, uint64_t kept, uint64_t total) noexcept {
    _classify_ticks = ticks;
    _segments_kept = kept;
    _segments_total = total;
  }

  void AddStats(uint64_t ticks) noexcept {
    _stats_ticks.fetch_add(ticks, std::memory_order_relaxed);
  }

  void AddLookup(uint64_t ticks, uint64_t rows, uint64_t dropped) noexcept {
    _lookup_ticks.fetch_add(ticks, std::memory_order_relaxed);
    _lookup_rows.fetch_add(rows, std::memory_order_relaxed);
    _lookup_dropped.fetch_add(dropped, std::memory_order_relaxed);
  }

  void FinishInit();

  void Report(duckdb::OperatorMetrics& metrics);

 private:
  Info Render() const;
  duckdb::ExplainNode Prepared(bool draws_filter) const;
  duckdb::ExplainNode Executed() const;
  duckdb::ExplainNode Checked(std::vector<duckdb::ExplainNode> nodes) const;
  duckdb::ExplainNode LookedUp(uint64_t rows) const;

  irs::FilterTree _filter;
  ScanChecks _checks;
  std::string _lookup_source;
  irs::QueryProfile _query;
  uint64_t _snapshot_ticks = 0;
  uint64_t _classify_ticks = 0;
  uint64_t _segments_kept = 0;
  uint64_t _segments_total = 0;
  std::atomic_uint64_t _stats_ticks = 0;
  std::atomic_uint64_t _lookup_ticks = 0;
  std::atomic_uint64_t _lookup_rows = 0;
  std::atomic_uint64_t _lookup_dropped = 0;
  std::atomic_uint64_t _init_ticks = 0;
  std::atomic_uint64_t _version = 0;
};

std::unique_ptr<irs::IndexSource> ProfileLookup(
  ScanProfile* profile, std::unique_ptr<irs::IndexSource> source);

}  // namespace sdb::connector
