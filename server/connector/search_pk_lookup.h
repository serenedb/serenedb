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

#include <duckdb/common/types/vector.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/duckdb_engine.h"
#include "basics/exceptions.h"
#include "catalog/table_options.h"

namespace sdb::connector {

inline std::pair<const irs::ColReader*, const irs::ColumnReader*>
SegmentPkColumn(const irs::IndexReader& reader, size_t seg_idx) noexcept {
  if (seg_idx >= reader.size()) {
    return {nullptr, nullptr};
  }
  const auto* col_reader = reader[seg_idx].GetColReader();
  if (!col_reader) {
    return {nullptr, nullptr};
  }
  const auto* pk_col = col_reader->Column(
    static_cast<irs::field_id>(catalog::Column::kGeneratedPKId));
  if (!pk_col) {
    return {nullptr, nullptr};
  }
  return {col_reader, pk_col};
}

}  // namespace sdb::connector
