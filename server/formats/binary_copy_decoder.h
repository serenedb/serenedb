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

#include <cstddef>
#include <cstdint>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/client_context.hpp>
#include <exception>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "pg/copy_in_batches.h"
#include "pg/deserialize.h"

namespace sdb::formats {

inline constexpr size_t kNoColumn = std::numeric_limits<size_t>::max();

struct CopyRowError {
  std::exception_ptr error;
  uint64_t line = 0;
  size_t column = kNoColumn;
};

// Decodes the tuples of COPY FROM in PostgreSQL's binary format: a walk over
// their length-prefixed fields, then PostgreSQL's binary input functions one
// column at a time.
class BinaryCopyDecoder {
 public:
  BinaryCopyDecoder(duckdb::ClientContext& context, pg::CopyInTarget target);

  void Start(const pg::CopyBatch& batch);
  bool Active() const noexcept { return static_cast<bool>(_batch); }
  bool Exhausted() const noexcept {
    return _segment == _batch->segments.size();
  }
  uint64_t Seq() const noexcept { return _seq; }
  void Stop() noexcept { _batch = nullptr; }
  duckdb::idx_t Parse(duckdb::DataChunk& output, duckdb::idx_t row);

  bool Failed() const noexcept { return static_cast<bool>(_error.error); }
  uint64_t FailedLine() const noexcept { return _error.line; }
  [[noreturn]] void ThrowError(uint64_t line) const;
  uint64_t Lines() const noexcept { return _lines; }

 private:
  std::span<std::string_view> Column(size_t column, size_t rows) noexcept {
    return {_fields.data() + column * STANDARD_VECTOR_SIZE, rows};
  }
  size_t Walk(std::string_view data, size_t max_rows);
  duckdb::idx_t Decode(duckdb::DataChunk& output, duckdb::idx_t row,
                       size_t rows);
  void FailRow(uint64_t line, std::exception_ptr error, size_t column);

  pg::CopyInTarget _target;
  std::vector<pg::DeserializationFunction<pg::ColumnSink>> _decoders;
  pg::DeserializeContext _dctx;
  std::vector<std::string_view> _fields;
  std::vector<uint64_t> _failed;
  CopyRowError _error;
  uint64_t _lines = 0;
  int32_t _bad_fields = -1;
  const pg::CopyBatch* _batch = nullptr;
  uint64_t _seq = 0;
  size_t _segment = 0;
  size_t _pos = 0;
};

}  // namespace sdb::formats
