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

#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/types.hpp>
#include <iresearch/index/merge/merge_profile.hpp>
#include <span>
#include <string>
#include <vector>

#include "connector/segment_rewrite/merge_target.h"
#include "search/compaction/merge_planner.h"
#include "search/compaction/merge_registry.h"

namespace sdb::connector {

struct SegmentRewriteJob {
  MergeTarget target;
  search::MergeRoundPlan plan;
};

struct ExecutedMerge {
  search::MergeOutcome outcome = search::MergeOutcome::Running;
  irs::MergeStats stats{};
  std::string error{};
};

std::vector<std::string> SegmentRewriteColumnNames();
std::vector<duckdb::LogicalType> SegmentRewriteColumnTypes();

duckdb::InsertionOrderPreservingMap<std::string> SegmentRewriteParams(
  const SegmentRewriteJob& job);

void AppendSegmentRewriteActuals(
  std::span<const ExecutedMerge> merges, bool zero_times,
  duckdb::InsertionOrderPreservingMap<std::string>& params);

}  // namespace sdb::connector
