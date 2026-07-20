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

#include <cstring>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <limits>
#include <memory>
#include <string_view>
#include <vector>

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

struct PrimaryKeyBatch {
  enum class Kind : uint8_t {
    None,
    I64,
    I64I64,
    Struct,
  };

  Kind kind = Kind::None;
  std::vector<int64_t> files;
  std::vector<int64_t> rows;
  // Kind::Struct only: BORROWS the batch's flattened STRUCT key vector
  // (column_count rows); valid only until the next Materialize consumes it.
  duckdb::Vector* column = nullptr;
  duckdb::idx_t column_count = 0;

  size_t Size() const noexcept {
    return kind == Kind::Struct ? column_count : rows.size();
  }
  void Reset() {
    files.clear();
    rows.clear();
    column = nullptr;
    column_count = 0;
  }
  void Append(int64_t row) { rows.push_back(row); }
  void Append(int64_t file, int64_t row) {
    files.push_back(file);
    rows.push_back(row);
  }
};

class IndexSource {
 public:
  virtual ~IndexSource() = default;

  virtual PrimaryKeyBatch::Kind PkKind() const = 0;

  // Materializes source columns for `count` pks from `start`; returns the rows
  // produced (< count when compacted to survivors).
  virtual duckdb::idx_t Materialize(duckdb::ClientContext& context,
                                    PrimaryKeyBatch& batch, duckdb::idx_t start,
                                    duckdb::idx_t count,
                                    duckdb::DataChunk& output) = 0;
};

}  // namespace sdb::connector
