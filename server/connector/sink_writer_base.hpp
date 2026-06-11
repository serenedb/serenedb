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
#include <iresearch/utils/type_limits.hpp>

#include "basics/containers/flat_hash_set.h"
#include "catalog/table_options.h"

namespace sdb::connector {

struct ColumnDescriptor {
  catalog::Column::Id id;
  catalog::ColumnStoreMode store_mode;
  duckdb::LogicalType type;
};

struct ExpressionDescriptor {
  duckdb::LogicalType type;
  irs::field_id field_id = irs::field_limits::invalid();
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
