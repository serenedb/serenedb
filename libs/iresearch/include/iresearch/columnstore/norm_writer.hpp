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

// Per-row-group metadata + stats. byte_size picks the 1/2/4-byte
// ReadUnchecked path; sum + non_zero_count feed BM25's GetAvg.
struct NormRowGroupPointer {
  // 1 if max < 256, 2 if max < 65536, else 4.
  uint8_t byte_size = 0;
  uint64_t row_count = 0;
  uint32_t max = 0;
  uint64_t sum = 0;
  uint64_t non_zero_count = 0;
  // Byte offset in .cs where the raw payload starts.
  uint64_t file_offset = 0;
};

// Streams norms one row group at a time. Memory is O(row_group_size * 4)
// per writer (~480 KB at the default), not O(segment_docs * 4). Writer
// offers no read API; open a NormColumnReader against the .cs after
// Finalize.
class NormColumnWriter final {
 public:
  NormColumnWriter(field_id id, std::string name, uint64_t row_group_size,
                   IndexOutput& out);

  NormColumnWriter(const NormColumnWriter&) = delete;
  NormColumnWriter& operator=(const NormColumnWriter&) = delete;

  // Append one value at the next implicit doc position. Triggers a row
  // group flush when the staging buffer reaches row_group_size.
  void Append(uint32_t value);

  // Flush the trailing partial row group. Idempotent.
  void Finalize();

  field_id Id() const noexcept { return _id; }
  const std::string& Name() const noexcept { return _name; }

  // For Writer::Commit footer serialization (post-Finalize).
  const std::vector<NormRowGroupPointer>& Pointers() const noexcept {
    return _pointers;
  }

 private:
  void FlushRowGroup();

  field_id _id;
  std::string _name;
  uint64_t _row_group_size;
  IndexOutput* _out;

  std::vector<uint32_t> _pending;
  std::vector<NormRowGroupPointer> _pointers;
  bool _finalized = false;
};

}  // namespace columnstore
}  // namespace irs
