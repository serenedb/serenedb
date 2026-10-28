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
#include <absl/numeric/int128.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <magic_enum/magic_enum.hpp>
#include <utility>

#include "connector/column_id.h"
#include "search/compaction/copy_check.h"
#include "search/compaction/merge_policies.h"

namespace sdb::search {
namespace {

int64_t FirstRow(const irs::SubReader& segment) {
  const auto rowids = DescribeSegment(segment).rowids;
  return rowids.Empty() ? 0 : rowids.first;
}

std::string Segments(uint64_t count) {
  return absl::StrCat(count, count == 1 ? " segment" : " segments");
}

PlannedMerge DescribeMerge(std::string reason,
                           std::span<const irs::SubReader* const> segments) {
  PlannedMerge merge{.reason = std::move(reason)};
  irs::containers::FlatHashMap<irs::field_id, uint64_t> column_bytes;
  irs::containers::FlatHashSet<irs::field_id> fields;
  irs::containers::FlatHashSet<irs::field_id> ann;
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
  constexpr std::string_view kReason = "adjacent policy";
  for (const auto& candidates :
       PickMerges(MakeMergePolicy(settings), reader, {}, request.merges)) {
    if (candidates.size() == 1 &&
        !irs::HasRemovals(candidates.front()->Meta())) {
      break;
    }
    for (const auto* candidate : candidates) {
      picked.emplace(candidate->Meta().name);
    }
    plan.merges.emplace_back(DescribeMerge(std::string{kReason}, candidates));
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
                  : absl::StrCat("not selected by the ", kReason),
    });
  }
}

void PlaceAlone(const irs::SubReader& segment, const CopyChecker* checker,
                MergeRoundPlan& plan) {
  if (!checker) {
    plan.skipped.emplace_back(SkippedSegment{
      .name = segment.Meta().name,
      .reason = "alone in its group without deletes",
    });
    return;
  }
  const auto check = checker->Check(segment);
  if (!check.Rewrite()) {
    plan.skipped.emplace_back(SkippedSegment{
      .name = segment.Meta().name,
      .reason = absl::StrCat(
        "alone in its group without deletes, re-encoding would save ",
        check.SavedPercent(), "%"),
    });
    return;
  }
  const irs::SubReader* inputs[] = {&segment};
  auto& merge = plan.merges.emplace_back(DescribeMerge(
    check.codec_changed
      ? std::string{"a declared column codec changed"}
      : absl::StrCat("re-encoding saves ", check.SavedPercent(), "%"),
    inputs));
  merge.lone = irs::LoneSegment::Rewrite;
}

void PlaceInSteps(std::span<const irs::SubReader* const> group,
                  size_t inputs_max, std::string_view reason,
                  MergeRoundPlan& plan) {
  SDB_ASSERT(inputs_max >= 2);
  SDB_ASSERT(group.size() > inputs_max);
  const auto count = group.size();
  const auto merges = std::min((count - 2) / (inputs_max - 1), inputs_max);
  const auto inputs =
    std::min(count - inputs_max + merges, merges * inputs_max);
  size_t first = 0;
  uint64_t window = 0;
  for (size_t i = 0; i < inputs; ++i) {
    window += group[i]->live_docs_count();
  }
  auto lightest = window;
  for (size_t i = inputs; i < count; ++i) {
    window += group[i]->live_docs_count();
    window -= group[i - inputs]->live_docs_count();
    if (window < lightest) {
      lightest = window;
      first = i - inputs + 1;
    }
  }
  const auto step = absl::StrCat(reason, ", first step");
  for (size_t merge = 0, begin = first; merge < merges; ++merge) {
    const auto end = first + inputs * (merge + 1) / merges;
    plan.merges.emplace_back(
      DescribeMerge(step, group.subspan(begin, end - begin)));
    begin = end;
  }
  for (size_t i = 0; i < count; ++i) {
    if (i < first || i >= first + inputs) {
      plan.skipped.emplace_back(SkippedSegment{
        .name = group[i]->Meta().name,
        .reason = "later step",
      });
    }
  }
  plan.partial = true;
}

void PlaceInGroups(const irs::IndexReader& reader, uint32_t target,
                   std::string_view reason, size_t inputs_max,
                   const CopyChecker* checker, MergeRoundPlan& plan) {
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
      .first_row = FirstRow(segment),
      .live_docs = segment.live_docs_count(),
    });
    total += placed.live_docs;
  }
  absl::c_stable_sort(order, [](const Placed& lhs, const Placed& rhs) {
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
  const auto alone = [](const irs::Compaction& bucket) {
    return bucket.size() == 1 && !irs::HasRemovals(bucket.front()->Meta());
  };
  if (checker) {
    std::vector<const irs::SubReader*> lone;
    for (const auto& bucket : buckets) {
      if (alone(bucket)) {
        lone.emplace_back(bucket.front());
      }
    }
    checker->Warm(lone);
  }
  for (const auto& bucket : buckets) {
    if (bucket.empty()) {
      continue;
    }
    if (alone(bucket)) {
      PlaceAlone(*bucket.front(), checker, plan);
      continue;
    }
    if (bucket.size() > inputs_max) {
      PlaceInSteps(bucket, inputs_max, reason, plan);
      continue;
    }
    plan.merges.emplace_back(DescribeMerge(std::string{reason}, bucket));
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
  PlaceInGroups(reader, target, absl::StrCat("compact to ", Segments(target)),
                request.inputs_max, nullptr, plan);
}

void PlanOptimize(const irs::IndexReader& reader,
                  const MergeRoundRequest& request, MergeRoundPlan& plan) {
  auto target = std::max<uint32_t>(1, request.target_segments);
  if (!request.exact_target) {
    const auto by_size =
      reader.live_docs_count() /
      std::max<uint64_t>(1, kOptimizeRowGroups * request.row_group_size);
    target = static_cast<uint32_t>(std::clamp<uint64_t>(by_size, 1, target));
  }
  target = static_cast<uint32_t>(
    std::min<uint64_t>(target, std::max<size_t>(1, reader.size())));
  PlaceInGroups(reader, target, absl::StrCat("optimize to ", Segments(target)),
                request.inputs_max, request.copy_check, plan);
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
  if (candidates.size() == 1 && !irs::HasRemovals(candidates.front()->Meta())) {
    plan.notes.emplace_back(
      "a single segment without deletes has nothing to merge");
    return;
  }
  if (candidates.size() > request.inputs_max) {
    struct Ranked {
      int64_t first_row = 0;
      const irs::SubReader* segment = nullptr;
    };
    std::vector<Ranked> by_row;
    by_row.reserve(candidates.size());
    for (const auto* segment : candidates) {
      by_row.emplace_back(FirstRow(*segment), segment);
    }
    absl::c_stable_sort(by_row, [](const Ranked& lhs, const Ranked& rhs) {
      return lhs.first_row < rhs.first_row;
    });
    for (size_t i = 0; i < by_row.size(); ++i) {
      candidates[i] = by_row[i].segment;
    }
    PlaceInSteps(candidates, request.inputs_max, "listed segments", plan);
    return;
  }
  plan.merges.emplace_back(DescribeMerge("listed segments", candidates));
}

}  // namespace

std::optional<MergeMode> ParseMergeMode(std::string_view name) noexcept {
  return magic_enum::enum_cast<MergeMode>(
    name, [](char given, char enumerator) noexcept {
      return given == absl::ascii_tolower(enumerator);
    });
}

PlannedSegment DescribeSegment(const irs::SubReader& segment) {
  PlannedSegment result{
    .name = segment.Meta().name,
    .docs = segment.docs_count(),
    .live_docs = segment.live_docs_count(),
    .bytes = segment.Meta().byte_size,
  };
  if (const auto* column = segment.Column(connector::kGeneratedPKId);
      column && column->Type().id() == duckdb::LogicalTypeId::BIGINT) {
    const auto& stats = column->MergedStatistics();
    if (duckdb::NumericStats::HasMinMax(stats)) {
      result.rowids = {
        .first = duckdb::NumericStats::GetMin<int64_t>(stats),
        .last = duckdb::NumericStats::GetMax<int64_t>(stats),
      };
    }
  }
  return result;
}

uint64_t PlannedMerge::EstimatedOutputBytes() const noexcept {
  return static_cast<uint64_t>(irs::LiveBytes(bytes, live_docs, docs));
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
    case MergeMode::Optimize:
      PlanOptimize(reader, request, plan);
      break;
  }
  if (plan.partial) {
    plan.notes.emplace_back(absl::StrCat(
      "a merge in key order takes at most ", Segments(request.inputs_max),
      ", so a larger group is merged in steps"));
  }
  return plan;
}

std::vector<std::string> UnknownSegments(const irs::IndexReader& reader,
                                         std::span<const std::string> names) {
  irs::containers::FlatHashSet<std::string_view> present;
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
