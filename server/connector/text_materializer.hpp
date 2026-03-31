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

#include <velox/common/file/File.h>
#include <velox/common/memory/MemoryPool.h>
#include <velox/dwio/common/Reader.h>
#include <velox/vector/ComplexVector.h>

#include <span>
#include <string>
#include <vector>

#include "basics/fwd.h"
#include "catalog/table_options.h"

namespace sdb::connector {

class TextMaterializer {
 public:
  TextMaterializer(velox::memory::MemoryPool& pool,
                   std::shared_ptr<velox::ReadFile> source,
                   std::unique_ptr<velox::dwio::common::Reader> reader,
                   std::unique_ptr<velox::dwio::common::RowReader> row_reader,
                   velox::RowTypePtr output_type,
                   std::vector<catalog::Column::Id> column_ids);

  velox::RowVectorPtr ReadRows(std::span<std::string> row_keys,
                               velox::VectorPtr scores,
                               std::vector<velox::VectorPtr> offsets_per_field);

 private:
  velox::memory::MemoryPool& _pool;
  std::shared_ptr<velox::ReadFile> _source;
  std::unique_ptr<velox::dwio::common::Reader> _reader;
  std::unique_ptr<velox::dwio::common::RowReader> _row_reader;
  velox::RowTypePtr _output_type;
  int64_t _score_column_idx = -1;
  std::vector<int64_t> _offsets_column_indices;
};

}  // namespace sdb::connector
