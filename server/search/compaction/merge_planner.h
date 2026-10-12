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

#include <cstdint>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "search/maintenance.h"

namespace sdb::search {

enum class MergeMode : uint8_t {
  Background = 0,
  Compact,
  Segments,
};

std::string_view MergeModeName(MergeMode mode) noexcept;
std::optional<MergeMode> ParseMergeMode(std::string_view name) noexcept;

struct PlannedSegment {
  std::string name{};
  uint64_t docs = 0;
  uint64_t live_docs = 0;
  uint64_t bytes = 0;
  std::optional<int64_t> rowid_min{};
  std::optional<int64_t> rowid_max{};
};

struct PlannedMerge {
  std::string reason{};
  std::vector<PlannedSegment> inputs{};
  uint64_t docs = 0;
  uint64_t live_docs = 0;
  uint64_t bytes = 0;
  uint64_t columns = 0;
  uint64_t column_bytes = 0;
  irs::field_id largest_column = irs::field_limits::invalid();
  uint64_t largest_column_bytes = 0;
  uint64_t fields = 0;
  uint64_t ann_columns = 0;

  uint64_t EstimatedOutputBytes() const noexcept;
};

struct SkippedSegment {
  std::string name{};
  std::string reason{};
};

struct MergeRoundPlan {
  MergeMode mode = MergeMode::Background;
  uint64_t segments = 0;
  std::vector<PlannedMerge> merges{};
  std::vector<SkippedSegment> skipped{};
  std::vector<std::string> notes{};
};

struct MergeRoundRequest {
  MergeMode mode = MergeMode::Background;
  const TasksSettings* settings = nullptr;
  size_t merges = 1;
  uint32_t target_segments = 1;
  bool single_output = false;
  std::span<const std::string> segments{};
};

PlannedSegment DescribeSegment(const irs::SubReader& segment);

MergeRoundPlan PlanMergeRound(const irs::IndexReader& reader,
                              const MergeRoundRequest& request);

std::vector<std::string> UnknownSegments(const irs::IndexReader& reader,
                                         std::span<const std::string> names);

irs::CompactionPolicy ExplicitPolicy(std::span<const std::string> names);

}  // namespace sdb::search
