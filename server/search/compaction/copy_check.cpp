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

#include "search/compaction/copy_check.h"

#include <absl/algorithm/container.h>
#include <absl/numeric/int128.h>

#include <algorithm>
#include <atomic>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/formats/column/col_copy_check.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/utils/assert.hpp>

namespace sdb::search {
namespace {

struct Estimate {
  const irs::ColReader* reader = nullptr;
  const irs::ColumnReader* column = nullptr;
  uint64_t bytes = 0;
};

class EstimateTask final : public duckdb::BaseExecutorTask {
 public:
  EstimateTask(duckdb::TaskExecutor& executor, std::span<const Estimate> work,
               std::atomic_size_t& next,
               const irs::IndexFieldOptions& options) noexcept
    : BaseExecutorTask{executor},
      _work{work},
      _next{&next},
      _options{&options} {}

  void ExecuteTask() final {
    for (auto i = _next->fetch_add(1, std::memory_order_relaxed);
         i < _work.size(); i = _next->fetch_add(1, std::memory_order_relaxed)) {
      irs::EstimateReencode(*_work[i].reader, *_work[i].column, *_options);
    }
  }

 private:
  std::span<const Estimate> _work;
  std::atomic_size_t* _next;
  const irs::IndexFieldOptions* _options;
};

}  // namespace

bool ColumnCopyCheck::Saves() const noexcept {
  return reencoded_bytes &&
         absl::uint128{*reencoded_bytes} * 100 <
           absl::uint128{bytes} * (100 - kReencodeMinSavingsPercent);
}

bool SegmentCopyCheck::Rewrite() const noexcept {
  return codec_changed || absl::uint128{saved_bytes} * 100 >
                            absl::uint128{bytes} * kReencodeMinSavingsPercent;
}

uint64_t SegmentCopyCheck::SavedPercent() const noexcept {
  return static_cast<uint64_t>(absl::uint128{saved_bytes} * 100 /
                               std::max<uint64_t>(bytes, 1));
}

std::vector<ColumnCopyCheck> CheckColumns(
  const irs::SubReader& segment, const irs::IndexFieldOptions& options) {
  std::vector<ColumnCopyCheck> checks;
  const auto* reader = segment.GetColReader();
  SDB_ASSERT(reader);
  checks.reserve(reader->Columns().size());
  for (const auto& column : reader->Columns()) {
    auto& check = checks.emplace_back(ColumnCopyCheck{
      .column = column->Id(),
      .rows = column->RowCount(),
      .bytes = irs::StoredBytes(*column),
    });
    const auto estimate = irs::EstimateReencode(*reader, *column, options);
    if (!estimate) {
      continue;
    }
    check.reencoded_bytes = static_cast<uint64_t>(
      absl::uint128{check.bytes} * estimate->reencoded_bytes /
      std::max<uint64_t>(1, estimate->stored_bytes));
    check.codec_changed = estimate->codec_changed;
  }
  return checks;
}

SegmentCopyCheck CheckSegment(const irs::SubReader& segment,
                              const irs::IndexFieldOptions& options) {
  SegmentCopyCheck result;
  for (const auto& check : CheckColumns(segment, options)) {
    result.bytes += check.bytes;
    result.codec_changed |= check.codec_changed;
    if (check.Saves()) {
      result.saved_bytes += check.bytes - *check.reencoded_bytes;
    }
  }
  return result;
}

void CopyChecker::Warm(std::span<const irs::SubReader* const> segments) const {
  std::vector<Estimate> work;
  for (const auto* segment : segments) {
    const auto* reader = segment->GetColReader();
    SDB_ASSERT(reader);
    for (const auto& column : reader->Columns()) {
      work.emplace_back(Estimate{
        .reader = reader,
        .column = column.get(),
        .bytes = irs::StoredBytes(*column),
      });
    }
  }
  absl::c_stable_sort(work, [](const Estimate& lhs, const Estimate& rhs) {
    return lhs.bytes > rhs.bytes;
  });
  std::atomic_size_t next{0};
  duckdb::TaskExecutor executor{*_scheduler};
  const auto tasks = std::min<size_t>(
    work.size(), std::max<size_t>(1, _scheduler->NumberOfThreads()));
  for (size_t task = 0; task != tasks; ++task) {
    executor.ScheduleTask(
      duckdb::make_uniq<EstimateTask>(executor, work, next, *_options));
  }
  executor.WorkOnTasks();
}

}  // namespace sdb::search
