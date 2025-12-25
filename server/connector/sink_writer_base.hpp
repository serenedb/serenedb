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
#include "rocksdb/slice.h"
#include "velox/type/Type.h"
#include "catalog/table_options.h"

namespace sdb::connector {

class SinkWriterBase {
 public:

  SinkWriterBase() = default;
  virtual ~SinkWriterBase() = default;

  virtual void Init(size_t batch_size /*row type + column ids ?*/) {}

  virtual void Write(std::span<const rocksdb::Slice> cell_slices,
             std::string_view full_key) = 0;

  virtual void Delete(std::string_view full_key) = 0;

  virtual void SwitchColumn(velox::TypeKind kind,  sdb::catalog::Column::Id column_id) {}

  virtual void Finish() {};
};

} // namespace sdb::connector
