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

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/storage/data_pointer.hpp>

#include "iresearch/types.hpp"

namespace duckdb {

class DataChunk;
class HyperLogLog;

}  // namespace duckdb
namespace irs {

class WriteContext;
struct FooterColumnEntry;

struct Chunk {
  duckdb::Vector data;
  size_t count;
};

class ColumnWriter final {
 public:
  ColumnWriter(field_id id, duckdb::LogicalType type, uint32_t row_group_size,
               WriteContext& write_ctx, FooterColumnEntry& entry,
               bool skip_validity, bool hyperloglog);

  ColumnWriter(const ColumnWriter&) = delete;
  ColumnWriter& operator=(const ColumnWriter&) = delete;

  void Append(uint64_t start_row, const duckdb::Vector& vec,
              duckdb::idx_t count);

  void Append(uint64_t start_row, const duckdb::Vector& vec,
              const duckdb::SelectionVector& sel, duckdb::idx_t count);

  template<typename Fill>
  void PushInStaging(uint64_t row, Fill&& fill) {
    PadNullsTo(row);
    auto& back = OpenChunk();
    fill(back.data, back.count);
    ++back.count;
    ++_data_ctx.filled;
    MaybeFlushRowGroup();
  }

  field_id Id() const noexcept { return _id; }
  const duckdb::LogicalType& Type() const noexcept { return _type; }
  uint32_t RowGroupSize() const noexcept { return _row_group_size; }
  bool SkipValidity() const noexcept { return _skip_validity; }
  bool HasHyperLogLog() const noexcept;
  duckdb::CompressionType Compression() const noexcept {
    return _forced_compression;
  }

  void SetCompression(duckdb::CompressionType compression) noexcept {
    _forced_compression = compression;
  }

  void Finalize();

  void SetHyperLogLog(duckdb::shared_ptr<duckdb::HyperLogLog> hll);

  void PadNullsTo(uint64_t target_row);

  struct DataContext {
    static constexpr size_t kInitialSize = 256;

    std::vector<Chunk> chunks;
    std::vector<duckdb::VectorCache> chunk_caches;
    size_t used_chunks = 0;
    size_t filled = 0;
    size_t first_rg_doc_id = 0;
    size_t next_capacity = kInitialSize;
  };

 private:
  Chunk& OpenChunk();
  void MaybeFlushRowGroup();
  void FlushChunks(uint64_t count);

  field_id _id;
  duckdb::LogicalType _type;
  uint32_t _row_group_size;
  WriteContext* _write_ctx;
  FooterColumnEntry* _entry;
  DataContext _data_ctx;
  bool _skip_validity = false;
  bool _hyperloglog = false;
  duckdb::CompressionType _forced_compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
};

}  // namespace irs
