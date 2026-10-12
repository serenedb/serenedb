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

#include "search/compaction/merge_planner.h"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/numeric/int128.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <utility>

#include "connector/column_id.h"
#include "search/compaction/merge_policies.h"

namespace sdb::search {
namespace {

bool HasDeletes(const irs::SubReader& segment) noexcept {
  return segment.live_docs_count() < segment.docs_count();
}

std::string Segments(uint64_t count) {
  return absl::StrCat(count, count == 1 ? " segment" : " segments");
}

PlannedMerge DescribeMerge(std::string reason,
                           std::span<const irs::SubReader* const> segments) {
  PlannedMerge merge{.reason = std::move(reason)};
  absl::flat_hash_map<irs::field_id, uint64_t> column_bytes;
  absl::flat_hash_set<irs::field_id> fields;
  absl::flat_hash_set<irs::field_id> ann;
  for (const auto* segment : segments) {
    auto& input = merge.inputs.emplace_back(DescribeSegment(*segment));
    merge.docs += input.docs;
    merge.live_docs += input.live_docs;
    merge.bytes += input.bytes;
    for (const auto id : segment->field_ids()) {
      fields.insert(id);
    }
    const auto* col_reader = segment->GetColReader();
    SDB_ASSERT(col_reader);
    for (const auto& column : col_reader->Columns()) {
      uint64_t bytes = 0;
      for (const auto& block : column->DataBlocks()) {
        bytes += block.byte_size;
      }
      column_bytes[column->Id()] += bytes;
      if (segment->Ann(column->Id())) {
        ann.insert(column->Id());
      }
    }
  }
  merge.columns = column_bytes.size();
  merge.fields = fields.size();
  merge.ann_columns = ann.size();
  for (const auto& [id, bytes] : column_bytes) {
    merge.column_bytes += bytes;
    if (bytes > merge.largest_column_bytes ||
        (bytes == merge.largest_column_bytes && id < merge.largest_column)) {
      merge.largest_column = id;
      merge.largest_column_bytes = bytes;
    }
  }
  return merge;
}

void PlanBackground(const irs::IndexReader& reader,
                    const MergeRoundRequest& request, MergeRoundPlan& plan) {
  SDB_ASSERT(request.settings);
  const auto& settings = *request.settings;
  if (settings.compaction_interval_msec == 0) {
    plan.notes.emplace_back(
      "background compaction is disabled (compaction_interval = 0)");
  }
  irs::CompactingSegments picked;
  const auto policy = MakeMergePolicy(settings);
  const auto reason =
    absl::StrCat(MergePolicyName(settings.compaction_policy), " policy");
  for (size_t merge = 0; merge < request.merges; ++merge) {
    irs::Compaction candidates;
    policy(candidates, reader, picked);
    if (candidates.empty() ||
        (candidates.size() == 1 && !HasDeletes(*candidates.front()))) {
      break;
    }
    for (const auto* candidate : candidates) {
      picked.emplace(candidate->Meta().name);
    }
    plan.merges.emplace_back(DescribeMerge(reason, candidates));
  }
  for (const auto& segment : reader) {
    const auto& meta = segment.Meta();
    if (picked.contains(meta.name)) {
      continue;
    }
    plan.skipped.emplace_back(SkippedSegment{
      .name = meta.name,
      .reason = meta.byte_size >= settings.compaction_max_segments_bytes
                  ? "at or above the merge size cap"
                  : absl::StrCat("not selected by the ", reason),
    });
  }
}

void PlanCompact(const irs::IndexReader& reader,
                 const MergeRoundRequest& request, MergeRoundPlan& plan) {
  const auto target =
    request.single_output ? 1U : std::max<uint32_t>(1, request.target_segments);
  if (reader.size() <= target) {
    plan.notes.emplace_back(absl::StrCat("already at most ", Segments(target)));
    return;
  }
  struct Placed {
    const irs::SubReader* segment;
    int64_t first_row;
    uint64_t live_docs;
  };
  std::vector<Placed> order;
  order.reserve(reader.size());
  absl::uint128 total = 0;
  for (const auto& segment : reader) {
    const auto& placed = order.emplace_back(Placed{
      .segment = &segment,
      .first_row = DescribeSegment(segment).rowid_min.value_or(0),
      .live_docs = segment.live_docs_count(),
    });
    total += placed.live_docs;
  }
  std::stable_sort(order.begin(), order.end(),
                   [](const Placed& lhs, const Placed& rhs) {
                     return lhs.first_row < rhs.first_row;
                   });
  std::vector<irs::Compaction> buckets(target);
  size_t group = 0;
  absl::uint128 taken = 0;
  for (size_t i = 0; i < order.size(); ++i) {
    const auto& placed = order[i];
    if (!buckets[group].empty() && group + 1 < target) {
      const bool middle_past_share =
        (2 * taken + placed.live_docs) * target >= 2 * (group + 1) * total;
      const bool one_per_group_left = order.size() - i == target - group - 1;
      if (middle_past_share || one_per_group_left) {
        ++group;
      }
    }
    buckets[group].emplace_back(placed.segment);
    taken += placed.live_docs;
  }
  const auto reason = absl::StrCat("compact to ", Segments(target));
  for (const auto& bucket : buckets) {
    if (bucket.size() == 1 && !HasDeletes(*bucket.front())) {
      plan.skipped.emplace_back(SkippedSegment{
        .name = bucket.front()->Meta().name,
        .reason = "alone in its group without deletes",
      });
      continue;
    }
    plan.merges.emplace_back(DescribeMerge(reason, bucket));
  }
}

void PlanSegments(const irs::IndexReader& reader,
                  const MergeRoundRequest& request, MergeRoundPlan& plan) {
  irs::Compaction candidates;
  for (const auto& segment : reader) {
    if (absl::c_linear_search(request.segments, segment.Meta().name)) {
      candidates.emplace_back(&segment);
    }
  }
  SDB_ASSERT(!candidates.empty());
  if (candidates.size() == 1 && !HasDeletes(*candidates.front())) {
    plan.notes.emplace_back(
      "a single segment without deletes has nothing to merge");
    return;
  }
  plan.merges.emplace_back(DescribeMerge("listed segments", candidates));
}

}  // namespace

std::string_view MergeModeName(MergeMode mode) noexcept {
  switch (mode) {
    case MergeMode::Background:
      return "background";
    case MergeMode::Compact:
      return "compact";
    case MergeMode::Segments:
      return "segments";
  }
  return "unknown";
}

std::optional<MergeMode> ParseMergeMode(std::string_view name) noexcept {
  for (const auto mode :
       {MergeMode::Background, MergeMode::Compact, MergeMode::Segments}) {
    if (MergeModeName(mode) == name) {
      return mode;
    }
  }
  return std::nullopt;
}

PlannedSegment DescribeSegment(const irs::SubReader& segment) {
  PlannedSegment result{
    .name = segment.Meta().name,
    .docs = segment.docs_count(),
    .live_docs = segment.live_docs_count(),
    .bytes = segment.Meta().byte_size,
  };
  if (const auto* column = segment.Column(connector::kGeneratedPKId)) {
    const auto& stats = column->MergedStatistics();
    if (duckdb::NumericStats::HasMinMax(stats)) {
      result.rowid_min = duckdb::NumericStats::GetMin<int64_t>(stats);
      result.rowid_max = duckdb::NumericStats::GetMax<int64_t>(stats);
    }
  }
  return result;
}

uint64_t PlannedMerge::EstimatedOutputBytes() const noexcept {
  if (docs == 0) {
    return 0;
  }
  return static_cast<uint64_t>(static_cast<long double>(bytes) *
                               static_cast<long double>(live_docs) /
                               static_cast<long double>(docs));
}

MergeRoundPlan PlanMergeRound(const irs::IndexReader& reader,
                              const MergeRoundRequest& request) {
  MergeRoundPlan plan{.mode = request.mode, .segments = reader.size()};
  switch (request.mode) {
    case MergeMode::Background:
      PlanBackground(reader, request, plan);
      break;
    case MergeMode::Compact:
      PlanCompact(reader, request, plan);
      break;
    case MergeMode::Segments:
      PlanSegments(reader, request, plan);
      break;
  }
  return plan;
}

std::vector<std::string> UnknownSegments(const irs::IndexReader& reader,
                                         std::span<const std::string> names) {
  absl::flat_hash_set<std::string_view> present;
  for (const auto& segment : reader) {
    present.emplace(segment.Meta().name);
  }
  std::vector<std::string> unknown;
  for (const auto& name : names) {
    if (!present.contains(name)) {
      unknown.emplace_back(name);
    }
  }
  return unknown;
}

irs::CompactionPolicy ExplicitPolicy(std::span<const std::string> names) {
  return [names = std::vector<std::string>{names.begin(), names.end()}](
           irs::Compaction& candidates, const irs::IndexReader& reader,
           const irs::CompactingSegments&) {
    for (const auto& segment : reader) {
      if (absl::c_linear_search(names, segment.Meta().name)) {
        candidates.emplace_back(&segment);
      }
    }
  };
}

irs::MergeGroups PickMerges(const irs::CompactionPolicy& policy,
                            const irs::IndexReader& index,
                            irs::CompactingSegments taken, size_t wanted) {
  irs::MergeGroups groups;
  for (size_t merge = 0; merge != wanted; ++merge) {
    irs::Compaction candidates;
    policy(candidates, index, taken);
    if (candidates.empty()) {
      break;
    }
    for (const auto* candidate : candidates) {
      taken.emplace(candidate->Meta().name);
    }
    groups.emplace_back(std::move(candidates));
  }
  return groups;
}

}  // namespace sdb::search
