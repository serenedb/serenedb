////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "index_utils.hpp"

#include <utility>

#include "iresearch/formats/segment_meta_writer.hpp"
#include "iresearch/index/index_meta.hpp"

namespace irs::index_utils {
namespace {

// Returns percentage of live documents
inline double FillFactor(const SegmentInfo& segment) noexcept {
  return static_cast<double>(segment.live_docs_count) /
         static_cast<double>(segment.docs_count);
}

// Returns approximated size of a segment in the absence of removals
inline size_t SizeWithoutRemovals(const SegmentInfo& segment,
                                  double fill_factor) noexcept {
  return static_cast<size_t>(static_cast<double>(segment.byte_size) *
                             fill_factor);
}

inline size_t SizeWithoutRemovals(const SegmentInfo& segment) noexcept {
  return SizeWithoutRemovals(segment, FillFactor(segment));
}

void PrepareFlush(IndexSegment& segment, bool increment_version) {
  auto& meta = segment.meta;
  SDB_ASSERT(meta.byte_size);  // Ensure segment size is estimated
  SDB_ASSERT(segment.meta.docs_mask_size <= segment.meta.byte_size);

  SDB_ASSERT(!meta.docs_mask || !meta.docs_mask->Empty());
  meta.live_docs_count = meta.docs_count;
  if (const auto removals = RemovalCount(meta); removals != 0) {
    SDB_ASSERT(removals < meta.docs_count);
    meta.live_docs_count -= removals;
    meta.version += uint64_t{increment_version};
  }
}

}  // namespace

CompactionPolicy MakePolicy(const CompactionBytes& options) {
  return [options](Compaction& candidates, const IndexReader& reader,
                   const CompactingSegments& compacting_segments) {
    const auto byte_threshold = options.threshold;
    size_t all_segment_bytes_size = 0;
    const auto segment_count = reader.size();

    for (auto& segment : reader) {
      all_segment_bytes_size += segment.Meta().byte_size;
    }

    const auto threshold = std::clamp(byte_threshold, 0.f, 1.f);
    const auto threshold_bytes_avg =
      (static_cast<float>(all_segment_bytes_size) /
       static_cast<float>(segment_count)) *
      threshold;

    // merge segment if: {threshold} > segment_bytes / (all_segment_bytes /
    // #segments)
    for (auto& segment : reader) {
      if (compacting_segments.contains(segment.Meta().name)) {
        continue;
      }
      const auto segment_bytes_size = segment.Meta().byte_size;
      if (threshold_bytes_avg >= static_cast<float>(segment_bytes_size)) {
        candidates.emplace_back(&segment);
      }
    }
  };
}

CompactionPolicy MakePolicy(const CompactionBytesAccum& options) {
  return [options](Compaction& candidates, const IndexReader& reader,
                   const CompactingSegments& compacting_segments) {
    auto byte_threshold = options.threshold;
    size_t all_segment_bytes_size = 0;
    std::vector<std::pair<size_t, const SubReader*>> segments;
    segments.reserve(reader.size());

    for (auto& segment : reader) {
      if (compacting_segments.contains(segment.Meta().name)) {
        continue;  // segment is already under compaction
      }
      segments.emplace_back(SizeWithoutRemovals(segment.Meta()), &segment);
      all_segment_bytes_size += segments.back().first;
    }

    size_t cumulative_size = 0;
    const auto threshold_size = static_cast<float>(all_segment_bytes_size) *
                                std::clamp(byte_threshold, 0.f, 1.f);

    // prefer to compact smaller segments
    absl::c_sort(segments, [](const auto& lhs, const auto& rhs) {
      return lhs.first < rhs.first;
    });

    // merge segment if: {threshold} >= (segment_bytes +
    // sum_of_merge_candidate_segment_bytes) / all_segment_bytes
    for (auto& entry : segments) {
      const auto segment_bytes_size = entry.first;

      if (static_cast<float>(cumulative_size + segment_bytes_size) <=
          threshold_size) {
        cumulative_size += segment_bytes_size;
        candidates.emplace_back(entry.second);
      }
    }
  };
}

CompactionPolicy MakePolicy(const CompactionCount& options) {
  return [options](Compaction& candidates, const IndexReader& reader,
                   const CompactingSegments& compacting_segments) {
    // merge first 'threshold' segments not already owned by another compaction
    for (size_t i = 0, count = std::min(options.threshold, reader.size());
         i < count; ++i) {
      auto& segment = reader[i];
      if (compacting_segments.contains(segment.Meta().name)) {
        continue;
      }
      candidates.emplace_back(&segment);
    }
  };
}

CompactionPolicy MakePolicy(const CompactionDocsFill& options) {
  return [options](Compaction& candidates, const IndexReader& reader,
                   const CompactingSegments& compacting_segments) {
    auto fill_threshold = options.threshold;
    auto threshold = std::clamp(fill_threshold, 0.f, 1.f);

    // merge segment if: {threshold} >= #segment_docs{valid} /
    // (#segment_docs{valid} + #segment_docs{removed})
    for (auto& segment : reader) {
      auto& meta = segment.Meta();
      if (compacting_segments.contains(meta.name)) {
        continue;
      }
      if (!meta.live_docs_count  // if no valid doc_ids left in segment
          || static_cast<float>(meta.docs_count) * threshold >=
               static_cast<float>(meta.live_docs_count)) {
        candidates.emplace_back(&segment);
      }
    }
  };
}

CompactionPolicy MakePolicy(const CompactionDocsLive& options) {
  return [options](Compaction& candidates, const IndexReader& meta,
                   const CompactingSegments& compacting_segments) {
    const auto docs_threshold = options.threshold;
    const auto all_segment_docs_count = meta.live_docs_count();
    const auto segment_count = meta.size();

    const auto threshold = std::clamp<float>(docs_threshold, 0, 1);
    const auto threshold_docs_avg =
      (static_cast<float>(all_segment_docs_count) /
       static_cast<float>(segment_count)) *
      threshold;

    // merge segment if: {threshold} >= segment_docs{valid} /
    // (all_segment_docs{valid} / #segments)
    for (auto& segment : meta) {
      auto& info = segment.Meta();
      if (compacting_segments.contains(info.name)) {
        continue;
      }
      if (!info.live_docs_count  // if no valid doc_ids left in segment
          || threshold_docs_avg >= static_cast<float>(info.live_docs_count)) {
        candidates.emplace_back(&segment);
      }
    }
  };
}

void FlushIndexSegment(Directory& dir, IndexSegment& segment,
                       bool increment_version) {
  PrepareFlush(segment, increment_version);
  segment_meta::Write(dir, segment.filename, segment.meta);
}

void FlushIndexSegmentPatch(Directory& dir, IndexSegment& segment,
                            const DocumentMask& patch) {
  const auto parent = segment.meta.version;
  PrepareFlush(segment, true);
  segment_meta::Write(dir, segment.filename, segment.meta, &patch, parent);
}

}  // namespace irs::index_utils
