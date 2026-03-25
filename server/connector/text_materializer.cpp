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

#include "text_materializer.hpp"

#include <velox/common/base/BitUtil.h>
#include <velox/dwio/common/Mutation.h>
#include <velox/dwio/text/reader/TextReader.h>

#include "basics/down_cast.h"
#include "connector/primary_key.hpp"

namespace sdb::connector {

TextMaterializer::TextMaterializer(
  velox::memory::MemoryPool& pool, std::shared_ptr<velox::ReadFile> source,
  std::unique_ptr<velox::dwio::common::Reader> reader,
  std::unique_ptr<velox::dwio::common::RowReader> row_reader,
  velox::RowTypePtr output_type)
  : _pool{pool},
    _source{std::move(source)},
    _reader{std::move(reader)},
    _row_reader{std::move(row_reader)},
    _output_type{std::move(output_type)} {}

velox::RowVectorPtr TextMaterializer::ReadRows(
  std::span<const std::string> row_keys, velox::VectorPtr /*scores*/) {
  if (row_keys.empty()) {
    return nullptr;
  }
  SDB_ASSERT(std::is_sorted(row_keys.begin(), row_keys.end()));

  auto decode = [](std::string_view key) {
    return primary_key::ReadSigned<int64_t>(key);
  };

  auto& text_row_reader =
    basics::downCast<facebook::velox::text::TextRowReader>(*_row_reader);

  velox::vector_size_t total = row_keys.size();
  velox::VectorPtr output =
    velox::BaseVector::create<velox::RowVector>(_output_type, total, &_pool);
  for (size_t i = 0; i < row_keys.size(); ++i) {
    auto file_offset = decode(row_keys[i]);
    text_row_reader.nextAtOffset(file_offset, i, 1, output);
  }

  if (output->size() == 0) {
    return nullptr;
  }

  return std::dynamic_pointer_cast<velox::RowVector>(output);
}

}  // namespace sdb::connector
