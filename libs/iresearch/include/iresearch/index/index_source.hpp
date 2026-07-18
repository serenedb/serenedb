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
#include <duckdb/common/vector.hpp>
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
    // A user-specified external lookup key: one STRUCT column of arbitrary
    // field types/count. The batch does NOT copy it -- `column` borrows the
    // already-read key vector for the current batch (see below).
    Struct,
  };

  Kind kind = Kind::None;
  duckdb::vector<int64_t> files;
  duckdb::vector<int64_t> rows;
  // Kind::Struct only: a BORROWED pointer to the current batch's key column (a
  // flattened STRUCT vector, `column_count` rows). Not owned, no per-row copy,
  // no duckdb::Value -- valid only until the immediately-following Materialize
  // consumes it. One batch is read per Materialize, so no accumulation.
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

  // Materializes the source columns for `count` pks starting at `start` and
  // returns the number of output rows produced -- equal to `count` unless a
  // pushed lookup-column filter compacted the batch to survivors.
  virtual duckdb::idx_t Materialize(duckdb::ClientContext& context,
                                    PrimaryKeyBatch& batch, duckdb::idx_t start,
                                    duckdb::idx_t count,
                                    duckdb::DataChunk& output) = 0;
};

}  // namespace sdb::connector
