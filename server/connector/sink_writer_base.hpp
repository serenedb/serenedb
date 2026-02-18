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

#include <velox/type/Type.h>

#include "catalog/table_options.h"
#include "rocksdb/slice.h"

namespace sdb::connector {

// Interface for all index writers used in DataSink
class SinkIndexWriter {
 public:
  SinkIndexWriter() = default;
  virtual ~SinkIndexWriter() = default;

  virtual void Init(size_t batch_size) {}

  virtual void Finish() = 0;
  virtual void Abort() = 0;

  // returns true if writer is interested in this column
  virtual bool SwitchColumn(const velox::Type& type, bool have_nulls,
                            sdb::catalog::Column::Id column_id) {
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

// Base implementation of column centric index writers
class ColumnSinkWriterImplBase {
 public:
  ColumnSinkWriterImplBase(std::span<const catalog::Column::Id> columns) {
    _columns.reserve(columns.size());
    for (auto c : columns) {
      _columns.insert(c);
    }
    SDB_ASSERT(!_columns.empty());
  }

  bool IsIndexed(sdb::catalog::Column::Id column_id) const noexcept {
    return _columns.contains(column_id);
  }

 protected:
  containers::FlatHashSet<catalog::Column::Id> _columns;
};

}  // namespace sdb::connector
