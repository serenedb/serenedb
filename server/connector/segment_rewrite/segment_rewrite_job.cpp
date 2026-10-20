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

#include "connector/segment_rewrite/segment_rewrite_job.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>

#include <array>
#include <duckdb/common/string_util.hpp>
#include <iresearch/utils/enum_name.hpp>
#include <magic_enum/magic_enum.hpp>

namespace sdb::connector {
namespace {

constexpr size_t kListedNames = 8;

std::string Bytes(uint64_t bytes) {
  return duckdb::StringUtil::BytesToHumanReadableString(bytes);
}

std::string Duration(uint64_t nanos) {
  return absl::StrFormat("%.1f ms", static_cast<double>(nanos) / 1e6);
}

template<typename Range, typename Format>
std::string JoinLimited(const Range& items, Format format) {
  std::vector<std::string> parts;
  size_t count = 0;
  for (const auto& item : items) {
    if (count++ < kListedNames) {
      parts.emplace_back(format(item));
    }
  }
  if (count > kListedNames) {
    parts.emplace_back(absl::StrCat("+", count - kListedNames, " more"));
  }
  return absl::StrJoin(parts, ", ");
}

std::string RowIdRange(const search::PlannedMerge& merge) {
  irs::SegmentRange rowids;
  for (const auto& input : merge.inputs) {
    if (input.rowids.Empty()) {
      return {};
    }
    rowids = irs::Union(rowids, input.rowids);
  }
  if (rowids.Empty()) {
    return {};
  }
  return absl::StrCat(", rowid ", rowids.first, "..", rowids.last);
}

std::string Plural(uint64_t count, std::string_view noun) {
  return absl::StrCat(count, " ", noun, count == 1 ? "" : "s");
}

void DescribeMerge(size_t number, const search::PlannedMerge& merge,
                   const MergeTarget& target,
                   duckdb::InsertionOrderPreservingMap<std::string>& params) {
  const auto key = absl::StrCat("Merge ", number);
  params[key] = absl::StrCat(Plural(merge.inputs.size(), "segment"), " -> 1, ",
                             merge.reason);
  params[absl::StrCat(key, " Inputs")] =
    JoinLimited(merge.inputs,
                [](const search::PlannedSegment& input) { return input.name; });
  params[absl::StrCat(key, " Docs")] = absl::StrCat(
    merge.docs, " (", merge.live_docs, " live)", RowIdRange(merge));
  params[absl::StrCat(key, " Bytes")] = absl::StrCat(
    Bytes(merge.bytes), " -> est ", Bytes(merge.EstimatedOutputBytes()));
  auto columns = absl::StrCat(merge.columns, ", ", Bytes(merge.column_bytes));
  if (merge.columns != 0) {
    absl::StrAppend(&columns, ", largest ",
                    target.ColumnName(merge.largest_column), " ",
                    Bytes(merge.largest_column_bytes));
  }
  params[absl::StrCat(key, " Columns")] = std::move(columns);
  params[absl::StrCat(key, " Postings")] = Plural(merge.fields, "field");
  if (merge.ann_columns != 0) {
    params[absl::StrCat(key, " ANN")] = Plural(merge.ann_columns, "column");
  }
}

std::string StepLabel(irs::MergeStep step) {
  switch (step) {
    case irs::MergeStep::Prepare:
      return "Prepare";
    case irs::MergeStep::Norms:
      return "Norms";
    case irs::MergeStep::Columns:
      return "Columns";
    case irs::MergeStep::ColumnsCommit:
      return "Columns Commit";
    case irs::MergeStep::Ann:
      return "ANN";
    case irs::MergeStep::Postings:
      return "Postings";
    case irs::MergeStep::Commit:
      return "Commit";
  }
  return std::string{irs::SnakeCaseName(step)};
}

}  // namespace

std::vector<std::string> SegmentRewriteColumnNames() {
  std::vector<std::string> names{
    "merge",       "outcome", "inputs",      "input_docs",   "input_live_docs",
    "input_bytes", "output",  "output_docs", "output_bytes", "total_ms",
  };
  for (size_t step = 0; step < irs::kMergeStepCount; ++step) {
    names.emplace_back(absl::StrCat(
      irs::SnakeCaseName(static_cast<irs::MergeStep>(step)), "_ms"));
  }
  names.emplace_back("error");
  return names;
}

std::vector<duckdb::LogicalType> SegmentRewriteColumnTypes() {
  std::vector<duckdb::LogicalType> types{
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::VARCHAR,
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::VARCHAR,
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::DOUBLE,
  };
  types.insert(types.end(), irs::kMergeStepCount, duckdb::LogicalType::DOUBLE);
  types.emplace_back(duckdb::LogicalType::VARCHAR);
  return types;
}

duckdb::InsertionOrderPreservingMap<std::string> SegmentRewriteParams(
  const SegmentRewriteJob& job) {
  const auto& plan = job.plan;
  duckdb::InsertionOrderPreservingMap<std::string> params;
  params["Table"] = job.target.Name();
  params["Mode"] = std::string{irs::SnakeCaseName(plan.mode)};
  params["Segments"] = std::to_string(plan.segments);
  params["Merges"] = std::to_string(plan.merges.size());
  if (!plan.merges.empty()) {
    params["Pipeline"] =
      job.target.Sorted() ? "MERGE_ORDER, MERGE_UNITS" : "MERGE_UNITS";
  }
  for (size_t i = 0; i < plan.merges.size(); ++i) {
    DescribeMerge(i + 1, plan.merges[i], job.target, params);
  }
  if (!plan.skipped.empty()) {
    params["Not Merged"] =
      JoinLimited(plan.skipped, [](const search::SkippedSegment& skipped) {
        return absl::StrCat(skipped.name, " (", skipped.reason, ")");
      });
  }
  if (!plan.notes.empty()) {
    params["Notes"] = absl::StrJoin(plan.notes, "; ");
  }
  return params;
}

void AppendSegmentRewriteActuals(
  std::span<const ExecutedMerge> merges, bool zero_times,
  duckdb::InsertionOrderPreservingMap<std::string>& params) {
  std::array<size_t, magic_enum::enum_count<search::MergeOutcome>()> outcomes{};
  irs::MergeProfile total;
  uint64_t written = 0;
  for (const auto& merge : merges) {
    ++outcomes[static_cast<size_t>(merge.outcome)];
    written += merge.stats.output_bytes;
    if (zero_times) {
      continue;
    }
    for (size_t step = 0; step < irs::kMergeStepCount; ++step) {
      const auto id = static_cast<irs::MergeStep>(step);
      total.Add(id, merge.stats.profile.Nanos(id));
    }
  }
  std::vector<std::string> summary;
  for (size_t outcome = 0; outcome < outcomes.size(); ++outcome) {
    if (outcomes[outcome] != 0) {
      summary.emplace_back(absl::StrCat(
        irs::SnakeCaseName(static_cast<search::MergeOutcome>(outcome)), " ",
        outcomes[outcome]));
    }
  }
  params["Actual Outcome"] =
    summary.empty() ? std::string{"nothing ran"} : absl::StrJoin(summary, ", ");
  params["Actual Written"] = Bytes(written);
  for (size_t step = 0; step < irs::kMergeStepCount; ++step) {
    const auto id = static_cast<irs::MergeStep>(step);
    params[absl::StrCat("Actual ", StepLabel(id))] = Duration(total.Nanos(id));
  }
  params["Actual Total"] = Duration(total.TotalNanos());
}

}  // namespace sdb::connector
