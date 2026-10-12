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

#include <absl/time/time.h>

#include <cstdint>
#include <functional>
#include <iresearch/index/compaction/adjacent_run_policy.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "search/compaction/merge_registry.h"

namespace sdb::search {

class SimulatedSegment final : public irs::SubReader {
 public:
  explicit SimulatedSegment(
    irs::SegmentInfo meta,
    std::optional<irs::SegmentRange> range = std::nullopt) noexcept
    : _meta{std::move(meta)}, _range{range} {}

  static std::optional<irs::SegmentRange> RangeOf(
    const irs::SubReader& segment) noexcept {
    return static_cast<const SimulatedSegment&>(segment)._range;
  }

  const SimulatedSegment& operator*() const noexcept { return *this; }

  uint64_t CountMappedMemory() const final { return 0; }
  const irs::SegmentInfo& Meta() const final { return _meta; }
  const irs::DocumentMask* docs_mask() const final { return nullptr; }
  irs::lead::Node::ptr docs_iterator() const final { return nullptr; }
  std::span<const irs::field_id> field_ids() const final { return {}; }
  const irs::TermReader* field(irs::field_id) const final { return nullptr; }
  irs::NormReader::ptr norms(irs::field_id) const final { return {}; }

 private:
  irs::SegmentInfo _meta;
  std::optional<irs::SegmentRange> _range;
};

struct SimulatedFlush {
  absl::Time time;
  uint64_t relation_id = 0;
  uint64_t docs = 0;
  uint64_t live_docs = 0;
  uint64_t bytes = 0;
  std::optional<int64_t> rowid_min;
  std::optional<int64_t> rowid_max;
};

struct MergeSimulation {
  irs::CompactionPolicy policy;
  size_t threads = 1;
  size_t helpers_per_merge = 0;
  uint64_t small_bytes = uint64_t{64} << 20;
  absl::Duration aging = absl::Seconds(30);
  double bytes_per_second = double{uint64_t{16} << 20};
};

struct SimulatedEvent {
  absl::Duration at;
  uint64_t relation_id = 0;
  SegmentEvent type = SegmentEvent::Flush;
  std::string segment;
  std::vector<std::string> inputs;
  uint64_t docs = 0;
  uint64_t live_docs = 0;
  uint64_t bytes = 0;
  uint64_t input_bytes = 0;
  std::optional<int64_t> rowid_min;
  std::optional<int64_t> rowid_max;
  absl::Duration queued;
  absl::Duration started;
  size_t threads = 0;
  double score = 0;
  MergeLane lane = MergeLane::Small;
  size_t segments = 0;
  uint64_t segment_bytes = 0;
  std::optional<double> overlap;
  size_t merges_queued = 0;
  size_t merges_running = 0;
};

std::vector<SimulatedEvent> SimulateMerges(
  std::vector<SimulatedFlush> trace, const MergeSimulation& options,
  const std::function<void()>& check_interrupt = {});

}  // namespace sdb::search
