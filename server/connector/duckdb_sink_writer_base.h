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

#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>

#include "catalog/table_options.h"
#include "connector/sink_writer_base.hpp"
#include "rocksdb/slice.h"

namespace sdb::connector {

// DuckDB-native version of SinkIndexWriter.
// Same interface pattern, but uses DuckDB types instead of Velox.
class DuckDBSinkIndexWriter {
 public:
  DuckDBSinkIndexWriter() = default;
  virtual ~DuckDBSinkIndexWriter() = default;

  virtual void Init(duckdb::idx_t batch_size, const duckdb::DataChunk& input) {}

  virtual void Finish() = 0;
  virtual void Abort() = 0;

  // Switches the active column AND hands the implementation the typed
  // batch up front (e.g. for columnstore Append). Returns true if the
  // writer is interested in per-cell Write() calls for the column;
  // callers gate the per-cell loop on that. Per-cell-only paths (WAL
  // recovery, test fixtures) pass count == 0 and any Vector of the
  // matching type; the batch Append is a no-op at count == 0.
  virtual bool SwitchColumn(const ColumnDescriptor& col,
                            const duckdb::Vector& vec, duckdb::idx_t count) {
    SDB_ASSERT(false, "SwitchColumn call not implemented");
    return false;
  }

  // Writes a value of cell in column switched to by previous call to
  // SwitchColumn. Particular writer would not be called for cell values if
  // returned false from SwitchColumn.
  virtual void Write(std::span<const rocksdb::Slice> cell_slices,
                     std::string_view full_key) {
    SDB_ASSERT(false, "Write call not implemented");
  }

  // deletes row denoted by row_key. It is up to concrete writer to perform all
  // necessary deletes.
  virtual void DeleteRow(std::string_view row_key) {
    SDB_ASSERT(false, "DeleteRow call not implemented");
  }
};

// Base implementation of column centric index writers (same as Velox version)
class DuckDBColumnSinkWriterImplBase {
 public:
  DuckDBColumnSinkWriterImplBase(std::span<const catalog::Column::Id> columns) {
    _columns.reserve(columns.size());
    for (auto c : columns) {
      _columns.insert(c);
    }
    SDB_ASSERT(!_columns.empty());
  }

  bool IsIndexed(catalog::Column::Id column_id) const noexcept {
    return _columns.contains(column_id);
  }

 protected:
  containers::FlatHashSet<catalog::Column::Id> _columns;
};

}  // namespace sdb::connector
