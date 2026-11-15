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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/client_context.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "pg/copy_in_batches.h"
#include "pg/deserialize.h"

namespace sdb::connector {

using CopyDeserializers =
  std::vector<pg::DeserializationFunction<pg::VectorSink>>;

CopyDeserializers MakeCopyDeserializers(
  const std::vector<duckdb::LogicalType>& types, bool binary);

bool ProcessTextRow(std::string_view line, char delim,
                    std::string_view null_str, bool ignore_errors,
                    const CopyDeserializers& deserializers,
                    pg::DeserializeContext& dctx, duckdb::DataChunk& output,
                    duckdb::idx_t row, std::string& field_buf);

class CopyBatchParser {
 public:
  CopyBatchParser(duckdb::ClientContext& context, pg::CopyInTarget target,
                  std::atomic<uint64_t>& rejected);

  void Start(const pg::CopyBatch& batch);
  bool Active() const noexcept { return static_cast<bool>(_batch); }
  bool Exhausted() const noexcept {
    return _segment == _batch->segments.size();
  }
  uint64_t Seq() const noexcept { return _batch->seq; }
  void Stop() noexcept { _batch = nullptr; }
  duckdb::idx_t Parse(duckdb::DataChunk& output, duckdb::idx_t row);
  bool DropRejected(duckdb::DataChunk& output);

 private:
  duckdb::idx_t ParseText(duckdb::DataChunk& output, duckdb::idx_t row);
  duckdb::idx_t ParseBinary(duckdb::DataChunk& output, duckdb::idx_t row);
  void Reject(duckdb::idx_t row);

  pg::CopyInTarget _target;
  CopyDeserializers _deserializers;
  pg::DeserializeContext _dctx;
  std::string _field_buf;
  std::vector<duckdb::idx_t> _rejected_rows;
  std::atomic<uint64_t>& _rejected;
  const pg::CopyBatch* _batch = nullptr;
  size_t _segment = 0;
  size_t _pos = 0;
  bool _skip = false;
};

}  // namespace sdb::connector
