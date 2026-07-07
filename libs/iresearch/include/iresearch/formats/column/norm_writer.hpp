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
#include <span>
#include <vector>

#include "basics/containers/monotonic_buffer.hpp"
#include "iresearch/types.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace irs {

class IndexOutput;
class NormColumnWriter;

struct NormRowGroupMeta {
  uint8_t byte_size = 0;
  uint32_t row_count = 0;
  uint32_t max = 0;
  uint64_t sum = 0;
  uint64_t non_zero_count = 0;
  uint64_t file_offset = 0;
};

void SerializeNormColumn(duckdb::Serializer& s, const NormColumnWriter& nw);
std::vector<NormRowGroupMeta> DeserializeNormMetas(duckdb::Deserializer& d,
                                                   field_id id,
                                                   uint64_t footer_offset);

class NormColumnWriter final {
 public:
  NormColumnWriter(field_id id, uint32_t row_group_size, IndexOutput& out);

  NormColumnWriter(const NormColumnWriter&) = delete;
  NormColumnWriter& operator=(const NormColumnWriter&) = delete;

  void Append(uint64_t target_row, uint32_t value);

  // Bulk-append `count` raw norm values stored as `byte_size`-byte little-
  // endian in `src`, starting at `target_row`. Decodes into the same
  // uint32_t `_pending` buffer that `Append` uses; per-row stats updates
  // (max/sum/non_zero) and row-group flushes are batched per call instead
  // of paid once per row. `byte_size` must be 1, 2, or 4.
  void AppendBytes(uint64_t target_row, const byte_type* src, size_t count,
                   uint8_t byte_size);

  void PadTo(uint64_t target);

  void Finalize();

  field_id Id() const noexcept { return _id; }

  uint64_t RowCount() const noexcept;

  const auto& Pointers() const noexcept { return _pointers; }

 private:
  void FlushRowGroup();

  field_id _id;
  uint32_t _row_group_size;
  IndexOutput* _out;

  MonotonicBuffer<uint32_t, 1, 0> _pending;
  std::vector<std::span<const uint32_t>> _spans;
  uint32_t _filled = 0;
  uint64_t _flushed = 0;
  uint32_t _rg_max = 0;
  uint64_t _rg_sum = 0;
  uint64_t _rg_non_zero = 0;
  std::vector<NormRowGroupMeta> _pointers;
};

}  // namespace irs
