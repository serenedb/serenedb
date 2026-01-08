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

#include "basics/fwd.h"
#include "catalog/table_options.h"
#include "rocksdb/slice.h"
#include "velox/type/Type.h"

namespace sdb::connector {

class SinkWriterBase {
 public:
  SinkWriterBase() = default;
  virtual ~SinkWriterBase() = default;

  virtual void Init(size_t batch_size) {}

  virtual void SwitchColumn(velox::TypeKind kind, bool have_nulls,
                            sdb::catalog::Column::Id column_id) {}
  virtual void Write(std::span<const rocksdb::Slice> cell_slices,
                     std::string_view full_key) = 0;

  virtual void DeleteRow(std::string_view row_key) = 0;
  virtual void Delete(std::string_view row_key) = 0;

  virtual void Finish() {};
};

}  // namespace sdb::connector
