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
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/formats/column/col_copy_check.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

#include "query/config.h"

namespace sdb::search {
namespace {

constexpr uint64_t kOptimizeRowGroups = 4;
constexpr long double kBase = 6;
constexpr long double kMinBase = 2;
constexpr long double kRelaxFrom = 10;
constexpr long double kRelaxTo = 50;
constexpr long double kMaxDeletedShare = 0.25;

struct RunOptions {
  size_t max_segments = 0;
  uint64_t max_bytes = 0;
  uint64_t segment_cost = 0;
};

struct RunCandidate {
  const irs::SubReader* segment = nullptr;
  long double bytes = 0;
  uint64_t live_docs = 0;
  long double deleted_share = 0;
  bool taken = false;
};

struct Window {
  size_t begin = 0;
  size_t end = 0;
  long double score = std::numeric_limits<long double>::max();
};

RunCandidate DescribeCandidate(const irs::SubReader& segment, bool taken) {
  const auto& meta = segment.Meta();
  const auto live = static_cast<long double>(meta.live_docs_count) /
                    static_cast<long double>(meta.docs_count);
  return {
    .segment = &segment,
    .bytes = irs::LiveBytes(meta),
    .live_docs = meta.live_docs_count,
    .deleted_share = 1 - live,
    .taken = taken,
  };
}

long double BaseFor(const RunOptions& options, size_t run) noexcept {
  const auto relax = std::clamp(
    (static_cast<long double>(run) - kRelaxFrom) / (kRelaxTo - kRelaxFrom),
    0.0L, 1.0L);
  return std::min(kBase - (kBase - kMinBase) * relax,
                  static_cast<long double>(options.max_segments));
}

void ConsiderRun(std::span<const RunCandidate> segments, size_t begin,
                 size_t end, const RunOptions& options, Window& best) {
  const auto base = BaseFor(options, end - begin);
  const auto cost = static_cast<long double>(options.segment_cost);
  for (auto first = begin; first != end; ++first) {
    long double bytes = 0;
    long double floored = 0;
    long double largest = 0;
    uint64_t docs = 0;
    const auto last = std::min(end, first + options.max_segments);
    for (auto next = first; next != last; ++next) {
      const auto& segment = segments[next];
      bytes += segment.bytes;
      floored += std::max(segment.bytes, cost);
      largest = std::max(largest, std::max(segment.bytes, cost));
      docs += segment.live_docs;
      if (bytes > static_cast<long double>(options.max_bytes) ||
          docs > irs::kMergeDocsMax) {
        break;
      }
      const auto count = next - first + 1;
      const auto total = bytes + cost * static_cast<long double>(count);
      if (count < 2 || floored < base * largest) {
        continue;
      }
      const auto score = total / (static_cast<long double>(count) - 1.9L);
      if (score <= best.score) {
        best = Window{.begin = first, .end = next + 1, .score = score};
      }
    }
  }
}

irs::Compaction PickRun(std::span<RunCandidate> segments,
                        const RunOptions& options) {
  Window best;
  for (size_t begin = 0; begin != segments.size();) {
    if (segments[begin].taken) {
      ++begin;
      continue;
    }
    auto end = begin;
    while (end != segments.size() && !segments[end].taken) {
      ++end;
    }
    ConsiderRun(segments, begin, end, options, best);
    begin = end;
  }
  irs::Compaction candidates;
  if (best.begin != best.end) {
    for (auto i = best.begin; i != best.end; ++i) {
      segments[i].taken = true;
      candidates.emplace_back(segments[i].segment);
    }
    return candidates;
  }
  RunCandidate* purge = nullptr;
  auto share = kMaxDeletedShare;
  for (auto& segment : segments) {
    if (!segment.taken && segment.deleted_share > share) {
      purge = &segment;
      share = segment.deleted_share;
    }
  }
  if (purge) {
    purge->taken = true;
    candidates.emplace_back(purge->segment);
  }
  return candidates;
}

struct EstimateWork {
  const irs::ColReader* reader = nullptr;
  const irs::ColumnReader* column = nullptr;
  size_t check = 0;
};

class EstimateTask final : public duckdb::BaseExecutorTask {
 public:
  EstimateTask(duckdb::TaskExecutor& executor,
               std::span<const EstimateWork> work,
               std::span<irs::ReencodeEstimate> estimates,
               std::atomic_size_t& next,
               const irs::IndexFieldOptions& options) noexcept
    : BaseExecutorTask{executor},
      _work{work},
      _estimates{estimates},
      _next{&next},
      _options{&options} {}

  void ExecuteTask() final {
    for (auto i = _next->fetch_add(1, std::memory_order_relaxed);
         i < _work.size(); i = _next->fetch_add(1, std::memory_order_relaxed)) {
      _estimates[i] =
        irs::EstimateReencode(*_work[i].reader, *_work[i].column, *_options);
    }
  }

 private:
  std::span<const EstimateWork> _work;
  std::span<irs::ReencodeEstimate> _estimates;
  std::atomic_size_t* _next;
  const irs::IndexFieldOptions* _options;
};

std::vector<irs::ReencodeEstimate> CheckRewrites(
  const MergeRoundRequest& request,
  std::span<const irs::SubReader* const> segments) {
  SDB_ASSERT(request.scheduler && request.field_options);
  std::vector<irs::ReencodeEstimate> rewrites(segments.size());
  for (const auto& check :
       CheckColumns(*request.scheduler, *request.field_options, segments)) {
    const auto& column = check.estimate;
    const bool saves = irs::ReencodePays({
      .stored_bytes = column.stored_bytes,
      .reencoded_bytes = column.reencoded_bytes,
    });
    auto& rewrite = rewrites[check.segment];
    rewrite.stored_bytes += column.stored_bytes;
    rewrite.reencoded_bytes +=
      saves ? column.reencoded_bytes : column.stored_bytes;
    rewrite.codec_changed |= column.codec_changed;
  }
  return rewrites;
}

uint64_t SavedPercent(const irs::ReencodeEstimate& rewrite) {
  return static_cast<uint64_t>(
    absl::uint128{rewrite.stored_bytes - rewrite.reencoded_bytes} * 100 /
    std::max<uint64_t>(rewrite.stored_bytes, 1));
}

std::string Segments(uint64_t count) {
  return absl::StrCat(count, count == 1 ? " segment" : " segments");
}

PlannedMerge DescribeMerge(std::string reason,
                           std::span<const irs::SubReader* const> segments) {
  PlannedMerge merge{.reason = std::move(reason)};
  for (const auto* segment : segments) {
    const auto& input = merge.inputs.emplace_back(DescribeSegment(*segment));
    merge.docs += input.docs;
    merge.live_docs += input.live_docs;
    merge.bytes += input.bytes;
  }
  return merge;
}

void PlanBackground(const irs::IndexReader& reader,
                    const MergeRoundRequest& request, MergeRoundPlan& plan) {
  SDB_ASSERT(request.settings);
  const irs::SegmentNames none;
  for (const auto& candidates :
       PickMerges(*request.settings, request.inputs_max, reader,
                  irs::CompactingSegments{none}, {}, request.merges)) {
    if (candidates.size() == 1 &&
        !irs::HasRemovals(candidates.front()->Meta())) {
      break;
    }
    plan.merges.emplace_back(DescribeMerge("adjacent policy", candidates));
  }
}

void PlaceAlone(const irs::SubReader& segment,
                const irs::ReencodeEstimate* rewrite, MergeRoundPlan& plan) {
  if (!rewrite || !irs::ReencodePays(*rewrite)) {
    return;
  }
  const irs::SubReader* inputs[] = {&segment};
  plan.merges.emplace_back(DescribeMerge(
    rewrite->codec_changed
      ? std::string{"a declared column codec changed"}
      : absl::StrCat("re-encoding saves ", SavedPercent(*rewrite), "%"),
    inputs));
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
  plan.partial = true;
}

void PlaceInGroups(const irs::IndexReader& reader, uint32_t target,
                   std::string_view reason, const MergeRoundRequest& request,
                   MergeRoundPlan& plan) {
  const absl::uint128 total = reader.live_docs_count();
  std::vector<irs::Compaction> buckets(target);
  size_t group = 0;
  absl::uint128 taken = 0;
  size_t left = reader.size();
  for (const auto& segment : reader) {
    const uint64_t live_docs = segment.live_docs_count();
    if (!buckets[group].empty() && group + 1 < target) {
      const bool middle_past_share =
        (2 * taken + live_docs) * target >= 2 * (group + 1) * total;
      const bool one_per_group_left = left == target - group - 1;
      if (middle_past_share || one_per_group_left) {
        ++group;
      }
    }
    buckets[group].emplace_back(&segment);
    taken += live_docs;
    --left;
  }
  const auto alone = [](const irs::Compaction& bucket) {
    return bucket.size() == 1 && !irs::HasRemovals(bucket.front()->Meta());
  };
  const bool check = request.mode == MergeMode::Optimize;
  std::vector<irs::ReencodeEstimate> rewrites;
  if (check) {
    std::vector<const irs::SubReader*> lone;
    for (const auto& bucket : buckets) {
      if (alone(bucket)) {
        lone.emplace_back(bucket.front());
      }
    }
    rewrites = CheckRewrites(request, lone);
  }
  size_t next_lone = 0;
  for (const auto& bucket : buckets) {
    if (bucket.empty()) {
      continue;
    }
    if (alone(bucket)) {
      PlaceAlone(*bucket.front(), check ? &rewrites[next_lone++] : nullptr,
                 plan);
      continue;
    }
    if (bucket.size() > request.inputs_max) {
      PlaceInSteps(bucket, request.inputs_max, reason, plan);
      continue;
    }
    plan.merges.emplace_back(DescribeMerge(std::string{reason}, bucket));
  }
}

void PlanCompact(const irs::IndexReader& reader,
                 const MergeRoundRequest& request, MergeRoundPlan& plan) {
  const auto target = std::max<uint32_t>(1, request.target_segments);
  if (reader.size() <= target) {
    return;
  }
  PlaceInGroups(reader, target, absl::StrCat("compact to ", Segments(target)),
                request, plan);
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
                request, plan);
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
    return;
  }
  if (candidates.size() > request.inputs_max) {
    PlaceInSteps(candidates, request.inputs_max, "listed segments", plan);
    return;
  }
  plan.merges.emplace_back(DescribeMerge("listed segments", candidates));
}

}  // namespace

std::vector<ColumnCopyCheck> CheckColumns(
  duckdb::TaskScheduler& scheduler, const irs::IndexFieldOptions& options,
  std::span<const irs::SubReader* const> segments) {
  std::vector<ColumnCopyCheck> checks;
  std::vector<EstimateWork> work;
  for (size_t segment = 0; segment != segments.size(); ++segment) {
    const auto* reader = segments[segment]->GetColReader();
    SDB_ASSERT(reader);
    for (const auto& column : reader->Columns()) {
      const auto bytes = irs::StoredBytes(*column);
      const bool checked = !column->Type().IsNested();
      if (checked) {
        work.emplace_back(reader, column.get(), checks.size());
      }
      checks.emplace_back(segment, column->Id(),
                          irs::ReencodeEstimate{
                            .stored_bytes = bytes,
                            .reencoded_bytes = bytes,
                          },
                          checked);
    }
  }
  absl::c_stable_sort(work,
                      [&](const EstimateWork& lhs, const EstimateWork& rhs) {
                        return checks[lhs.check].estimate.stored_bytes >
                               checks[rhs.check].estimate.stored_bytes;
                      });
  std::vector<irs::ReencodeEstimate> estimates(work.size());
  std::atomic_size_t next{0};
  duckdb::TaskExecutor executor{scheduler};
  const auto tasks = std::min<size_t>(
    work.size(), std::max<size_t>(1, scheduler.NumberOfThreads()));
  for (size_t task = 0; task != tasks; ++task) {
    executor.ScheduleTask(duckdb::make_uniq<EstimateTask>(
      executor, work, estimates, next, options));
  }
  executor.WorkOnTasks();
  for (size_t i = 0; i != work.size(); ++i) {
    const auto& sampled = estimates[i];
    auto& estimate = checks[work[i].check].estimate;
    estimate.reencoded_bytes = static_cast<uint64_t>(
      absl::uint128{estimate.stored_bytes} * sampled.reencoded_bytes /
      std::max<uint64_t>(1, sampled.stored_bytes));
    estimate.codec_changed = sampled.codec_changed;
  }
  return checks;
}

PlannedSegment DescribeSegment(const irs::SubReader& segment) {
  return {
    .name = segment.Meta().name,
    .docs = segment.docs_count(),
    .live_docs = segment.live_docs_count(),
    .bytes = segment.Meta().byte_size,
  };
}

uint64_t PlannedMerge::EstimatedOutputBytes() const noexcept {
  return static_cast<uint64_t>(irs::LiveBytes(bytes, live_docs, docs));
}

uint32_t DefaultTarget(MergeMode mode, duckdb::ClientContext& context) {
  if (mode == MergeMode::Optimize) {
    return static_cast<uint32_t>(
      duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads());
  }
  return GetIntSetting(context, "sdb_compact_target_segments");
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

irs::MergeGroups PickMerges(const TasksSettings& settings, size_t inputs_max,
                            const irs::IndexReader& index,
                            const irs::CompactingSegments& unavailable,
                            std::span<const std::string> blocked,
                            size_t wanted) {
  const RunOptions options{
    .max_segments =
      std::min<size_t>(settings.compaction_max_segments, inputs_max),
    .max_bytes = settings.compaction_max_segments_bytes,
    .segment_cost = settings.compaction_floor_segment_bytes,
  };
  std::vector<RunCandidate> segments;
  segments.reserve(index.size());
  for (const auto& segment : index) {
    const auto& meta = segment.Meta();
    if (meta.live_docs_count == 0) {
      continue;
    }
    segments.emplace_back(
      DescribeCandidate(segment, unavailable.contains(meta.name) ||
                                   absl::c_linear_search(blocked, meta.name)));
  }
  irs::MergeGroups groups;
  for (size_t merge = 0; merge != wanted; ++merge) {
    auto candidates = PickRun(segments, options);
    if (candidates.empty()) {
      break;
    }
    groups.emplace_back(std::move(candidates));
  }
  return groups;
}

}  // namespace sdb::search
