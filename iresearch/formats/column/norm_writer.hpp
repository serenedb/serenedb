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

#include "iresearch/types.hpp"
#include "iresearch/utils/containers/monotonic_buffer.hpp"

namespace irs {

class IndexOutput;
class NormColumnWriter;

inline constexpr uint32_t kNormEscape = 255;
inline constexpr uint32_t kNormExceptionShare = 256;
inline constexpr size_t kNormExceptionBytes = 2 * sizeof(uint32_t);

struct NormRowGroupMeta {
  uint8_t byte_size = 0;
  uint32_t max = 0;
  uint64_t sum = 0;
  uint64_t non_zero_count = 0;
  uint64_t file_offset = 0;
  uint32_t exceptions = 0;
};

struct NormColumnMeta {
  uint32_t row_group_size = 0;
  uint64_t row_count = 0;
  std::vector<NormRowGroupMeta> row_groups;
};

class NormColumnWriter final {
 public:
  NormColumnWriter(field_id id, uint32_t row_group_size, IndexOutput& out);

  NormColumnWriter(const NormColumnWriter&) = delete;
  NormColumnWriter& operator=(const NormColumnWriter&) = delete;

  void Append(uint64_t target_row, uint32_t value);

  void AppendValues(uint64_t target_row, std::span<const uint32_t> values);

  void PadTo(uint64_t target);

  void Finalize();

  field_id Id() const noexcept { return _id; }

  uint64_t RowCount() const noexcept;

  uint32_t RowGroupSize() const noexcept { return _row_group_size; }

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
