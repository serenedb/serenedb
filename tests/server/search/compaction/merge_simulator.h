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
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <span>
#include <string>
#include <vector>

#include "search/compaction/merge_scheduler.h"

namespace sdb::search {

class SimulatedSegment final : public irs::SubReader {
 public:
  explicit SimulatedSegment(irs::SegmentInfo meta) noexcept
    : _meta{std::move(meta)} {}

  const SimulatedSegment& operator*() const noexcept { return *this; }

  uint64_t CountMappedMemory() const final { return 0; }
  const irs::SegmentInfo& Meta() const final { return _meta; }
  const irs::DocumentMask* docs_mask() const final { return nullptr; }
  irs::lead::Node::ptr docs_iterator() const final { return nullptr; }
  std::span<const irs::field_id> field_ids() const final { return {}; }
  std::span<const irs::TermReader* const> fields(irs::field_id) const final {
    return {};
  }
  irs::NormReader::ptr norms(irs::field_id) const final { return {}; }

 private:
  irs::SegmentInfo _meta;
};

struct SimulatedFlush {
  absl::Time time;
  uint64_t relation_id = 0;
  uint64_t docs = 0;
  uint64_t live_docs = 0;
  uint64_t bytes = 0;
};

using MergePicker = std::function<irs::MergeGroups(
  const irs::IndexReader& index, const irs::CompactingSegments& taken,
  size_t wanted)>;

struct MergeSimulation {
  MergePicker pick;
  size_t threads = 1;
  double bytes_per_second = double{uint64_t{16} << 20};
};

enum class SimulatedEventType : uint8_t {
  Flush = 0,
  Merge,
};

struct SimulatedEvent {
  absl::Duration at;
  uint64_t relation_id = 0;
  SimulatedEventType type = SimulatedEventType::Flush;
  std::string segment;
  std::vector<std::string> inputs;
  uint64_t docs = 0;
  uint64_t live_docs = 0;
  uint64_t bytes = 0;
  uint64_t input_bytes = 0;
  absl::Duration queued;
  absl::Duration started;
  size_t threads = 0;
  double score = 0;
  MergeLane lane = MergeLane::Small;
  size_t segments = 0;
  uint64_t segment_bytes = 0;
  size_t merges_queued = 0;
  size_t merges_running = 0;
};

std::vector<SimulatedEvent> SimulateMerges(std::vector<SimulatedFlush> trace,
                                           const MergeSimulation& options);

}  // namespace sdb::search
