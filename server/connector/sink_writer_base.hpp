////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <iresearch/types.hpp>

#include "basics/containers/flat_hash_set.h"
#include "catalog/table_options.h"

namespace sdb::connector {

// Per-column descriptor passed to writers when switching to a new column.
// Carries both catalog-level attributes (id, store_mode) and per-call runtime
// facts (type, have_nulls) so SwitchColumn() takes a single argument and
// future additions don't churn signatures or call sites.
struct ColumnDescriptor {
  catalog::Column::Id id;
  catalog::ColumnStoreMode store_mode;
  duckdb::LogicalType type;
  bool have_nulls;
};

struct ExpressionDescriptor {
  duckdb::LogicalType type;
  bool have_nulls;
  irs::field_id field_id = 0;
};

// Base implementation of column centric index writers
class ColumnSinkWriterImplBase {
 public:
  ColumnSinkWriterImplBase(std::span<const catalog::Column::Id> columns) {
    _columns.reserve(columns.size());
    for (auto c : columns) {
      _columns.insert(c);
    }
  }

  bool IsIndexed(catalog::Column::Id column_id) const noexcept {
    return _columns.contains(column_id);
  }

 protected:
  containers::FlatHashSet<catalog::Column::Id> _columns;
};

}  // namespace sdb::connector
