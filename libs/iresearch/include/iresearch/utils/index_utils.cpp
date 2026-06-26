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

#include <cmath>
#include <set>
#include <utility>

#include "iresearch/formats/format_utils.hpp"
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

namespace tier {

struct SegmentStats {
  SegmentStats(const SubReader& reader, const SegmentInfo& meta) noexcept
    : reader{&reader},
      meta{&meta},
      fill_factor{FillFactor(meta)},
      size{SizeWithoutRemovals(meta, fill_factor)} {}

  operator const SubReader*() const noexcept { return reader; }

  const SubReader* reader;
  const SegmentInfo* meta;
  double fill_factor;
  size_t size;  // approximate size of segment without removals
};

bool operator<(const SegmentStats& lhs, const SegmentStats& rhs) noexcept {
  if (lhs.size != rhs.size) {
    return lhs.size < rhs.size;
  }
  if (lhs.fill_factor != rhs.fill_factor) {
    // TODO: why less deleted documents should be first?
    return lhs.fill_factor > rhs.fill_factor;
  }
  return lhs.meta->name < rhs.meta->name;
}

struct CompactionCandidate {
  using Iterator = std::vector<SegmentStats>::const_iterator;
  using Range = std::pair<Iterator, Iterator>;

  explicit CompactionCandidate(Iterator i) noexcept : segments{i, i} {}

  auto begin() const noexcept { return segments.first; }
  auto end() const noexcept { return segments.second; }

  Range segments;
  size_t count = 0;
  size_t size = 0;         // estimated size of the level
  double score = DBL_MIN;  // how good this permutation is
};

/// @returns score of the compaction bucket
double CompactionScore(const CompactionCandidate& compaction,
                       const size_t segments_per_tier,
                       const size_t floor_segment_bytes) noexcept {
  // to detect how skewed the compaction we do the following:
  // 1. evaluate coefficient of variation, less is better
  // 2. good candidates are in range [0;1]
  // 3. favor condidates where number of segments is equal to
  // 'segments_per_tier' approx
  // 4. prefer smaller compactions
  // 5. prefer compactions which clean removals

  switch (compaction.count) {
    case 0:
      // empty compaction makes not sense
      return 0;
    case 1: {
      auto& meta = *compaction.segments.first->meta;

      if (meta.docs_count == meta.live_docs_count) {
        // singletone without removals makes no sense
        return 0;
      }

      // FIXME honor number of deletes???
      // signletone with removals makes sense if nothing better is found
      return DBL_MIN + DBL_EPSILON;
    }
  }

  size_t size_before_compaction = 0;
  size_t size_after_compaction = 0;
  size_t size_after_compaction_floored = 0;
  for (auto& segment_stat : compaction) {
    size_before_compaction += segment_stat.meta->byte_size;
    size_after_compaction += segment_stat.size;
    size_after_compaction_floored +=
      std::max(segment_stat.size, floor_segment_bytes);
  }

  // evaluate coefficient of variation
  double sum_square_differences = 0;
  const auto segment_size_after_compaction_mean =
    static_cast<double>(size_after_compaction_floored) /
    static_cast<double>(compaction.count);
  for (auto& segment_stat : compaction) {
    const auto diff =
      static_cast<double>(std::max(segment_stat.size, floor_segment_bytes)) -
      segment_size_after_compaction_mean;
    sum_square_differences += diff * diff;
  }

  const auto stdev =
    std::sqrt(sum_square_differences / static_cast<double>(compaction.count));
  const auto cv = (stdev / segment_size_after_compaction_mean);

  // evaluate initial score
  auto score = 1. - cv;

  // favor compactions that contain approximately the requested number of
  // segments
  score *= std::pow(static_cast<double>(compaction.count) /
                      static_cast<double>(segments_per_tier),
                    1.5);

  // FIXME use relative measure, e.g. cosolidation_size/total_size
  // carefully prefer smaller compactions over the bigger ones
  score /= std::pow(size_after_compaction, 0.5);

  // favor compactions which clean out removals
  score /= std::pow(static_cast<double>(size_after_compaction) /
                      static_cast<double>(size_before_compaction),
                    2);

  return score;
}

}  // namespace tier
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

CompactionPolicy MakePolicy(const CompactionTier& options) {
  // can't merge less than 1 segment
  const auto max_segments_per_tier = std::max<size_t>(1, options.max_segments);
  // can't merge less than 1 segment
  // ensure min_segments_per_tier <= max_segments_per_tier
  const auto min_segments_per_tier =
    std::clamp<size_t>(options.min_segments, 1, max_segments_per_tier);
  const auto max_segments_bytes =
    std::max<size_t>(1, options.max_segments_bytes);
  const auto floor_segment_bytes =
    std::max<size_t>(1, options.floor_segment_bytes);
  // skip compaction that have score less than min_score
  const auto min_score = options.min_score;

  return [max_segments_per_tier, min_segments_per_tier, floor_segment_bytes,
          max_segments_bytes,
          min_score](Compaction& candidates, const IndexReader& reader,
                     const CompactingSegments& compacting_segments) {
    // total number of documents in index
    size_t total_docs_count = 0;
    // total number of live documents in index
    size_t total_live_docs_count = 0;

    /// Stage 1: get sorted list of segments and calculate overall stats

    std::vector<tier::SegmentStats> sorted_segments;
    sorted_segments.reserve(reader.size());

    // get segments from index meta
    for (auto& sub_reader : reader) {
      const auto& meta = sub_reader.Meta();
      if (!meta.live_docs_count) {
        // skip empty segments,
        // they'll be removed from index by index_writer during 'commit'
        continue;
      }

      total_live_docs_count += meta.live_docs_count;
      if (compacting_segments.contains(meta.name)) {
        total_docs_count += meta.live_docs_count;
        continue;
      }
      total_docs_count += meta.docs_count;
      sorted_segments.emplace_back(sub_reader, meta);
    }

    if (sorted_segments.size() < min_segments_per_tier) {
      return;
    }

    /// Stage 2: filter out "too large segments",
    //           segment is meant to be treated as large if
    /// - segment size is greater than 'max_segments_bytes / 2'
    /// - segment has many documents but only few deletions

    const auto total_fill_factor = static_cast<double>(total_live_docs_count) /
                                   static_cast<double>(total_docs_count);
    const auto too_big_segments_threshold = max_segments_bytes / 2;
    std::erase_if(sorted_segments, [&](const auto& segment) {
      return segment.size > too_big_segments_threshold &&
             segment.fill_factor > total_fill_factor;
    });

    if (sorted_segments.size() < min_segments_per_tier) {
      return;
    }

    /// Stage 3: sort candidates

    absl::c_sort(sorted_segments);

    /// Stage 4: find proper candidates

    tier::CompactionCandidate best(sorted_segments.begin());

    for (auto i = sorted_segments.begin(), end = sorted_segments.end();
         i != end; ++i) {
      tier::CompactionCandidate candidate(i);

      while (candidate.segments.second != end &&
             candidate.count < max_segments_per_tier) {
        candidate.size += candidate.segments.second->size;

        if (candidate.size > max_segments_bytes) {
          // overcome the limit
          break;
        }

        ++candidate.count;
        ++candidate.segments.second;

        if (candidate.count < min_segments_per_tier) {
          // not enough segments yet
          continue;
        }

        candidate.score = tier::CompactionScore(
          candidate, max_segments_per_tier, floor_segment_bytes);

        if (candidate.score < min_score) {
          // score is too small
          continue;
        }

        if (best.score < candidate.score) {
          best = candidate;
        }
      }
    }

    /// Stage 5: pick the best candidate
    absl::c_copy(best, std::back_inserter(candidates));
  };
}

void FlushIndexSegment(Directory& dir, IndexSegment& segment,
                       bool increment_version) {
  auto& meta = segment.meta;
  SDB_ASSERT(meta.codec);
  SDB_ASSERT(meta.byte_size);  // Ensure segment size is estimated
  SDB_ASSERT(segment.meta.docs_mask_size <= segment.meta.byte_size);

  meta.live_docs_count = meta.docs_count;
  if (const auto& docs_mask = meta.docs_mask; docs_mask) {
    SDB_ASSERT(!docs_mask->empty());
    SDB_ASSERT(docs_mask->size() < meta.docs_count);
    meta.live_docs_count -= static_cast<doc_id_t>(meta.docs_mask->size());
    meta.version += uint64_t{increment_version};
  }

  auto writer = segment.meta.codec->get_segment_meta_writer();
  writer->write(dir, segment.filename, segment.meta);
}

}  // namespace irs::index_utils
