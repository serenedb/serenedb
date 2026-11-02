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

#include "index/compaction/policy_compaction.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <utility>
#include <vector>

namespace tests {

CompactionPolicy MakePolicy(const CompactionBytes& options) {
  return [options](irs::Compaction& candidates, const irs::IndexReader& reader,
                   const irs::CompactingSegments& compacting_segments) {
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
  return [options](irs::Compaction& candidates, const irs::IndexReader& reader,
                   const irs::CompactingSegments& compacting_segments) {
    auto byte_threshold = options.threshold;
    size_t all_segment_bytes_size = 0;
    std::vector<std::pair<size_t, const irs::SubReader*>> segments;
    segments.reserve(reader.size());

    for (auto& segment : reader) {
      if (compacting_segments.contains(segment.Meta().name)) {
        continue;
      }
      segments.emplace_back(static_cast<size_t>(irs::LiveBytes(segment.Meta())),
                            &segment);
      all_segment_bytes_size += segments.back().first;
    }

    size_t cumulative_size = 0;
    const auto threshold_size = static_cast<float>(all_segment_bytes_size) *
                                std::clamp(byte_threshold, 0.f, 1.f);

    absl::c_sort(segments, [](const auto& lhs, const auto& rhs) {
      return lhs.first < rhs.first;
    });

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
  return [options](irs::Compaction& candidates, const irs::IndexReader& reader,
                   const irs::CompactingSegments& compacting_segments) {
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
  return [options](irs::Compaction& candidates, const irs::IndexReader& reader,
                   const irs::CompactingSegments& compacting_segments) {
    auto fill_threshold = options.threshold;
    auto threshold = std::clamp(fill_threshold, 0.f, 1.f);

    for (auto& segment : reader) {
      auto& meta = segment.Meta();
      if (compacting_segments.contains(meta.name)) {
        continue;
      }
      if (!meta.live_docs_count ||
          static_cast<float>(meta.docs_count) * threshold >=
            static_cast<float>(meta.live_docs_count)) {
        candidates.emplace_back(&segment);
      }
    }
  };
}

CompactionPolicy MakePolicy(const CompactionDocsLive& options) {
  return [options](irs::Compaction& candidates, const irs::IndexReader& meta,
                   const irs::CompactingSegments& compacting_segments) {
    const auto docs_threshold = options.threshold;
    const auto all_segment_docs_count = meta.live_docs_count();
    const auto segment_count = meta.size();

    const auto threshold = std::clamp<float>(docs_threshold, 0, 1);
    const auto threshold_docs_avg =
      (static_cast<float>(all_segment_docs_count) /
       static_cast<float>(segment_count)) *
      threshold;

    for (auto& segment : meta) {
      auto& info = segment.Meta();
      if (compacting_segments.contains(info.name)) {
        continue;
      }
      if (!info.live_docs_count ||
          threshold_docs_avg >= static_cast<float>(info.live_docs_count)) {
        candidates.emplace_back(&segment);
      }
    }
  };
}

irs::CompactionResult Compact(irs::IndexWriter& writer,
                              const CompactionPolicy& policy,
                              const irs::IndexFieldOptions* field_options,
                              const irs::MergeProgress& progress) {
  const irs::PipelineMergeExecutor executor{
    irs::DuckDBEngine::Instance().instance(), nullptr, 1};
  return Compact(writer, policy, field_options, progress, nullptr, executor);
}

irs::CompactionResult Compact(irs::IndexWriter& writer,
                              const CompactionPolicy& policy,
                              const irs::IndexFieldOptions* field_options,
                              const irs::MergeProgress& progress,
                              const irs::AnnBuildEnv* env,
                              const irs::PipelineMergeExecutor& executor) {
  bool busy = false;
  auto tickets = writer.ReserveMerges(
    [&policy](const irs::IndexReader& index,
              const irs::CompactingSegments& reserved,
              irs::MergeGroups& groups) {
      auto& group = groups.emplace_back();
      policy(group, index, reserved);
      if (group.size() == 1 && !irs::HasRemovals(group.front()->Meta())) {
        group.clear();
      }
    },
    busy);
  if (tickets.empty()) {
    return {.error =
              busy ? irs::CompactionError::Busy : irs::CompactionError::Ok};
  }
  SDB_ASSERT(tickets.size() == 1);
  return irs::GetReady(writer.RunMerge(std::move(tickets.front()),
                                       field_options, progress, env, executor));
}

}  // namespace tests
