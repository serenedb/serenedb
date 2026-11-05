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

#include <duckdb/parallel/task_scheduler.hpp>
#include <memory>
#include <span>

#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/index/merge/term_ranges.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

using MergedNormIdMap = containers::FlatHashMap<field_id, field_id>;

inline bool IsIndexField(const IndexFieldOptions* field_options, field_id id) {
  return !field_options || field_options->GetFieldKind(id) == FieldKind::Index;
}

FieldProperties MergedFieldProperties(field_id id, IndexFeatures features,
                                      const MergedNormIdMap& merged_norm_ids);

bool WritePostings(const FlushState& flush_state, PostingsOutputs outputs,
                   const MergeDocMap& inputs, bool sorted,
                   const IndexFieldOptions* field_options,
                   const MergeProgress& progress,
                   const MergedNormIdMap& merged_norm_ids, IdxWriter& idx,
                   std::span<const BasicTermReader* const> extra,
                   std::span<const std::unique_ptr<TermRangeMerge>> ranges,
                   uint64_t keyed_dicts, duckdb::TaskScheduler& scheduler,
                   size_t threads);

}  // namespace irs
