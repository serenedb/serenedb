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

#include "iresearch/formats/column/norm_column_reader.hpp"

#include <algorithm>
#include <utility>

#include "iresearch/store/data_input.hpp"

namespace irs {

NormColumnReader::NormColumnReader(field_id id,
                                   std::vector<NormRowGroupPointer> pointers,
                                   IndexInput& in)
  : _id{id}, _pointers{std::move(pointers)} {
  _row_offsets.reserve(_pointers.size() + 1);
  _row_offsets.push_back(0);
  _spans.resize(_pointers.size());

  size_t owned_total = 0;
  for (size_t rg = 0; rg < _pointers.size(); ++rg) {
    const auto& p = _pointers[rg];
    _total_row_count += p.row_count;
    SDB_ASSERT(_total_sum + p.sum >= _total_sum,
               "columnstore norm running sum overflow on column id ", _id);
    _total_sum += p.sum;
    _total_non_zero += p.non_zero_count;
    _row_offsets.push_back(_total_row_count);
    const auto byte_count = p.row_count * p.byte_size;
    if (byte_count == 0) {
      continue;
    }
    if (const auto* ptr = in.ReadData(p.file_offset, byte_count); ptr) {
      _spans[rg] = std::span<const byte_type>{ptr, byte_count};
    } else {
      _spans[rg] = {static_cast<const byte_type*>(nullptr), byte_count};
      owned_total += byte_count;
    }
  }
  if (owned_total != 0) {
    _owned.resize(owned_total);
    size_t offset = 0;
    for (size_t rg = 0; rg < _spans.size(); ++rg) {
      if (_spans[rg].data() != nullptr) {
        continue;
      }
      const auto byte_count = _spans[rg].size();
      if (byte_count == 0) {
        continue;
      }
      auto* dst = _owned.data() + offset;
      in.ReadBytes(_pointers[rg].file_offset, dst, byte_count);
      _spans[rg] = std::span<const byte_type>{dst, byte_count};
      offset += byte_count;
    }
  }
}

std::pair<size_t, uint64_t> NormColumnReader::Locate(
  uint64_t row_pos) const noexcept {
  SDB_ASSERT(row_pos < _total_row_count);
  auto it = std::upper_bound(_row_offsets.begin(), _row_offsets.end(), row_pos);
  const size_t rg = static_cast<size_t>((it - _row_offsets.begin()) - 1);
  return {rg, row_pos - _row_offsets[rg]};
}

uint32_t NormColumnReader::Get(uint64_t row_pos) const noexcept {
  SDB_ASSERT(row_pos < _total_row_count);
  auto [rg, in_rg] = Locate(row_pos);
  const auto byte_size = _pointers[rg].byte_size;
  SDB_ASSERT(!_spans[rg].empty());
  return ReadNormValue(_spans[rg].data() + in_rg * byte_size, byte_size);
}

}  // namespace irs
