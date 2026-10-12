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

#include "iresearch/index/compaction/adjacent_run_policy.hpp"

#include <algorithm>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/merge/merge_plan.hpp"

namespace irs {
namespace {

struct Candidate {
  const SubReader* segment = nullptr;
  long double bytes = 0;
  uint64_t live_docs = 0;
  long double deleted_share = 0;
  std::pair<int64_t, int64_t> order;
};

struct Window {
  size_t begin = 0;
  size_t end = 0;
  long double score = 0;
};

Candidate Describe(const SubReader& segment, const SegmentRangeOf& range_of) {
  const auto& meta = segment.Meta();
  const auto live = static_cast<long double>(meta.live_docs_count) /
                    static_cast<long double>(meta.docs_count);
  const auto range = range_of(segment).value_or(
    SegmentRange{.first = std::numeric_limits<int64_t>::max(),
                 .last = std::numeric_limits<int64_t>::max()});
  return {
    .segment = &segment,
    .bytes = static_cast<long double>(meta.byte_size) * live,
    .live_docs = meta.live_docs_count,
    .deleted_share = 1 - live,
    .order = {range.first, range.last},
  };
}

long double BaseFor(const AdjacentRunOptions& options, size_t run) noexcept {
  const auto relax =
    std::clamp((static_cast<long double>(run) -
                static_cast<long double>(options.relax_from)) /
                 (static_cast<long double>(options.relax_to) -
                  static_cast<long double>(options.relax_from)),
               0.0L, 1.0L);
  return options.base - (options.base - options.min_base) * relax;
}

void ConsiderRun(std::span<const Candidate> segments, size_t begin, size_t end,
                 const AdjacentRunOptions& options,
                 std::optional<Window>& best) {
  const auto base = BaseFor(options, end - begin);
  const auto cost = static_cast<long double>(options.segment_cost);
  for (auto first = begin; first != end; ++first) {
    long double bytes = 0;
    long double largest = 0;
    uint64_t docs = 0;
    const auto last = std::min(end, first + options.max_segments);
    for (auto next = first; next != last; ++next) {
      const auto& segment = segments[next];
      bytes += segment.bytes;
      largest = std::max(largest, segment.bytes);
      docs += segment.live_docs;
      if (bytes > static_cast<long double>(options.max_bytes) ||
          docs > options.max_docs) {
        break;
      }
      const auto count = next - first + 1;
      const auto total = bytes + cost * static_cast<long double>(count);
      if (count < 2 || total < base * (largest + cost)) {
        continue;
      }
      const auto score = total / (static_cast<long double>(count) - 1.9L);
      if (!best || score <= best->score) {
        best = Window{.begin = first, .end = next + 1, .score = score};
      }
    }
  }
}

}  // namespace

CompactionPolicy MakeAdjacentRunPolicy(AdjacentRunOptions options,
                                       SegmentRangeOf range_of) {
  options.max_docs = std::min(options.max_docs, kMergeDocsMax);
  return [options, range_of = std::move(range_of)](
           Compaction& candidates, const IndexReader& index,
           const CompactingSegments& compacting) {
    std::vector<Candidate> segments;
    segments.reserve(index.size());
    for (const auto& segment : index) {
      if (segment.Meta().live_docs_count != 0) {
        segments.emplace_back(Describe(segment, range_of));
      }
    }
    std::ranges::stable_sort(segments, {}, &Candidate::order);
    const auto taken = [&](size_t i) {
      return compacting.contains(segments[i].segment->Meta().name);
    };
    std::optional<Window> best;
    for (size_t begin = 0; begin != segments.size();) {
      if (taken(begin)) {
        ++begin;
        continue;
      }
      auto end = begin;
      while (end != segments.size() && !taken(end)) {
        ++end;
      }
      ConsiderRun(segments, begin, end, options, best);
      begin = end;
    }
    if (best) {
      for (auto i = best->begin; i != best->end; ++i) {
        candidates.emplace_back(segments[i].segment);
      }
      return;
    }
    const Candidate* purge = nullptr;
    auto share = static_cast<long double>(options.max_deleted_share);
    for (size_t i = 0; i != segments.size(); ++i) {
      if (!taken(i) && segments[i].deleted_share > share) {
        purge = &segments[i];
        share = purge->deleted_share;
      }
    }
    if (purge) {
      candidates.emplace_back(purge->segment);
    }
  };
}

}  // namespace irs
