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

#include <cstddef>
#include <cstdint>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/formats/column/reencode_cache.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <limits>
#include <span>
#include <string>
#include <vector>

#include "search/maintenance.h"

namespace sdb::search {

enum class MergeMode : uint8_t {
  Background = 0,
  Compact,
  Segments,
  Optimize,
};

struct PlannedSegment {
  std::string name;
  uint64_t docs = 0;
  uint64_t live_docs = 0;
  uint64_t bytes = 0;
};

struct PlannedMerge {
  std::string reason;
  std::vector<std::string> inputs;
  uint64_t docs = 0;
  uint64_t live_docs = 0;
  uint64_t bytes = 0;

  uint64_t EstimatedOutputBytes() const noexcept;
};

struct MergeRoundPlan {
  uint64_t segments = 0;
  std::vector<PlannedMerge> merges;
  bool partial = false;
};

struct MergeRoundRequest {
  MergeMode mode = MergeMode::Background;
  const TasksSettings& settings;
  size_t merges = 1;
  uint32_t target_segments = 1;
  bool exact_target = false;
  uint32_t row_group_size = 0;
  size_t inputs_max = std::numeric_limits<size_t>::max();
  std::span<const std::string> segments;
  const irs::IndexFieldOptions& field_options;
  duckdb::TaskScheduler& scheduler;
};

struct ColumnCopyCheck {
  size_t segment = 0;
  irs::field_id column = irs::field_limits::invalid();
  irs::ReencodeEstimate estimate;
  bool checked = false;
};

std::vector<ColumnCopyCheck> CheckColumns(
  duckdb::TaskScheduler& scheduler, const irs::IndexFieldOptions& options,
  std::span<const irs::SubReader* const> segments);

PlannedSegment DescribeSegment(const irs::SubReader& segment);

uint32_t DefaultTarget(MergeMode mode, duckdb::ClientContext& context);

MergeRoundPlan PlanMergeRound(const irs::IndexReader& reader,
                              const MergeRoundRequest& request);

std::vector<std::string> UnknownSegments(const irs::IndexReader& reader,
                                         std::span<const std::string> names);

irs::MergeGroups PickMerges(const TasksSettings& settings, size_t inputs_max,
                            const irs::IndexReader& index,
                            const irs::CompactingSegments& unavailable,
                            std::span<const std::string> blocked,
                            size_t wanted);

}  // namespace sdb::search
