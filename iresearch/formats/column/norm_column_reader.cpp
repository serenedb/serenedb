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

#include <utility>

#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {

NormColumnReader::NormColumnReader(field_id id, NormColumnMeta meta,
                                   IndexInput& in)
  : _id{id}, _pointers{std::move(meta.row_groups)} {
  _spans.resize(_pointers.size());
  if (!_pointers.empty()) {
    _rg_rows = meta.row_group_size;
    _total_row_count = meta.row_count;
  }

  size_t owned_total = 0;
  for (size_t rg = 0; rg < _pointers.size(); ++rg) {
    const auto& p = _pointers[rg];
    SDB_ASSERT(_total_sum + p.sum >= _total_sum,
               ".col reader norm running sum overflow on column id ", _id);
    _total_sum += p.sum;
    _total_non_zero += p.non_zero_count;
    _uniform_byte_size &= p.byte_size == _pointers.front().byte_size;
    const auto byte_count = RowGroupRowCount(rg) * p.byte_size;
    if (byte_count == 0) {
      continue;
    }
    if (const auto* ptr = in.ReadStable(p.file_offset, byte_count); ptr) {
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
      in.ReadData(_pointers[rg].file_offset, dst, byte_count);
      _spans[rg] = std::span<const byte_type>{dst, byte_count};
      offset += byte_count;
    }
  } else {
    _mapped = true;
  }
}

size_t NormColumnReader::Stream(size_t rg, const byte_type* from,
                                size_t advised) const noexcept {
  if (!_mapped) {
    return advised;
  }
  uint64_t budget = file_utils::kMaxReadahead;
  auto r = std::max(rg, advised);
  if (r == rg) {
    const auto span = _spans[r++];
    SDB_ASSERT(from >= span.data() && from < span.data() + span.size());
    const auto size = static_cast<size_t>(span.data() + span.size() - from);
    file_utils::Prefetch(from, size);
    budget -= std::min<uint64_t>(budget, size);
  }
  for (; r < _spans.size() && budget != 0; ++r) {
    const auto span = _spans[r];
    file_utils::Prefetch(span.data(), span.size());
    budget -= std::min<uint64_t>(budget, span.size());
  }
  return r;
}

uint32_t NormColumnReader::Get(uint64_t row_pos) const noexcept {
  const auto info = Locate(row_pos);
  SDB_ASSERT(!info.bytes.empty());
  return ReadNormValue(
    info.bytes.data() + (row_pos - info.first_row) * info.byte_size,
    info.byte_size);
}

}  // namespace irs
