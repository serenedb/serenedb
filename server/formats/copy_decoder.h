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
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "formats/text_splitter.h"
#include "pg/copy_in_batches.h"
#include "pg/deserialize.h"

namespace sdb::formats {

struct CopyTextEnd {
  std::atomic<uint64_t> marker{std::numeric_limits<uint64_t>::max()};
  std::atomic<uint64_t> latest{0};
};

class CopyDecoder {
 public:
  CopyDecoder(duckdb::ClientContext& context, pg::CopyInTarget target,
              CopyTextEnd& end, std::atomic<uint64_t>& rejected);

  void Start(const pg::CopyBatch& batch);
  bool Active() const noexcept { return static_cast<bool>(_batch); }
  bool Exhausted() const noexcept {
    return _segment == _batch->segments.size();
  }
  uint64_t Seq() const noexcept { return _batch->seq; }
  void Stop() noexcept { _batch = nullptr; }
  duckdb::idx_t Parse(duckdb::DataChunk& output, duckdb::idx_t row);

 private:
  duckdb::idx_t ParseText(duckdb::DataChunk& output, duckdb::idx_t row);
  duckdb::idx_t ParseBinary(duckdb::DataChunk& output, duckdb::idx_t row);
  void ResolveText(size_t rows);
  duckdb::idx_t Decode(duckdb::DataChunk& output, duckdb::idx_t row,
                       size_t rows);
  size_t WalkBinary(std::string_view data, size_t max_rows);
  void SkipHeader(std::string_view data);
  void CheckAfterMarker(std::string_view rest) const;
  void EndMarker();
  void CountRejected(uint64_t rejected);

  pg::CopyInTarget _target;
  std::vector<pg::DeserializationFunction<pg::ColumnSink>> _decoders;
  pg::DeserializeContext _dctx;
  TextSplitter _splitter;
  std::string _unescaped;
  std::vector<uint64_t> _failed;
  std::vector<uint64_t> _rejected_rows;
  CopyTextEnd& _end;
  std::atomic<uint64_t>& _rejected;
  const pg::CopyBatch* _batch = nullptr;
  size_t _segment = 0;
  size_t _pos = 0;
  bool _skip = false;
  bool _ended = false;
  bool _null_has_backslash = true;
};

}  // namespace sdb::formats
