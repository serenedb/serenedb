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

#include <iresearch/index/column_info.hpp>
#include <iresearch/index/compaction/adjacent_run_policy.hpp>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <optional>

#include "search/maintenance.h"

namespace sdb::search {

class InvertedIndexStorage;
class SearchTable;

std::optional<irs::SegmentRange> RowIdRange(const irs::SubReader& segment);

irs::CompactionPolicy MakeTierPolicy(const TasksSettings& settings);
irs::CompactionPolicy MakeAdjacentPolicy(const TasksSettings& settings,
                                         irs::SegmentRangeOf range_of);
irs::CompactionPolicy MakeMergePolicy(
  const TasksSettings& settings, irs::SegmentRangeOf range_of = RowIdRange);

struct CompactionOptions {
  bool alive = false;
  std::shared_ptr<const irs::IndexFieldOptions> keepalive;
  const irs::IndexFieldOptions* field_options = nullptr;
};

CompactionOptions PinCompactionOptions(InvertedIndexStorage& idx);
CompactionOptions PinCompactionOptions(SearchTable& table);

}  // namespace sdb::search
