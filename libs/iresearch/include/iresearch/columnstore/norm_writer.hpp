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
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/types.hpp"

namespace irs {

class IndexOutput;

namespace columnstore {

struct NormRowGroupPointer {
  uint8_t byte_size = 0;
  uint64_t row_count = 0;
  uint32_t max = 0;
  uint64_t sum = 0;
  uint64_t non_zero_count = 0;
  uint64_t file_offset = 0;
};

class NormColumnWriter final {
 public:
  NormColumnWriter(field_id id, uint64_t row_group_size, IndexOutput& out);

  NormColumnWriter(const NormColumnWriter&) = delete;
  NormColumnWriter& operator=(const NormColumnWriter&) = delete;

  void Append(uint64_t target_row, uint32_t value);

  void PadTo(uint64_t target);

  void Finalize();

  field_id Id() const noexcept { return _id; }

  uint64_t RowCount() const noexcept;

  const std::vector<NormRowGroupPointer>& Pointers() const noexcept {
    return _pointers;
  }

 private:
  void FlushRowGroup();

  field_id _id;
  uint64_t _row_group_size;
  IndexOutput* _out;

  std::vector<uint32_t> _pending;
  std::vector<NormRowGroupPointer> _pointers;
  bool _finalized = false;
};

}  // namespace columnstore
}  // namespace irs
