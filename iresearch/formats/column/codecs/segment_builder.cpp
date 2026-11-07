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

#include "iresearch/formats/column/codecs/segment_builder.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <absl/strings/match.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/operator/comparison_operators.hpp>
#include <limits>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text {
namespace {

using duckdb::BitpackingPrimitives;
using duckdb::string_t;

uint64_t PrefixKey(const string_t& value, size_t skip) noexcept {
  char buf[8] = {};
  const size_t size = value.GetSize();
  if (size > skip) {
    std::memcpy(buf, value.GetData() + skip,
                std::min<size_t>(size - skip, sizeof buf));
  }
  return absl::big_endian::Load64(buf);
}

size_t CommonPrefix(const string_t& a, const string_t& b) noexcept {
  return absl::FindLongestCommonPrefix({a.GetData(), a.GetSize()},
                                       {b.GetData(), b.GetSize()})
    .size();
}

}  // namespace

uint64_t SegmentBuilder::Estimate() const noexcept {
  return kHeaderSize + Packed(EntryCount(), _max_len) +
         CodesBytes(_shape, _rows, _entries.size(), _runs, _codes_tuning);
}

void SegmentBuilder::SortEntries() {
  const auto n = static_cast<uint32_t>(_entries.size());
  size_t common = _entries.empty() ? 0 : _entries[0].GetSize();
  for (uint32_t i = 1; i < n && common != 0; ++i) {
    common = std::min(common, CommonPrefix(_entries[0], _entries[i]));
  }
  _order.resize(n);
  for (uint32_t i = 0; i < n; ++i) {
    _order[i] = {PrefixKey(_entries[i], common), i};
  }
  absl::c_sort(_order, [&](const SortKey& a, const SortKey& b) {
    if (a.prefix != b.prefix) {
      return a.prefix < b.prefix;
    }
    return duckdb::LessThan::Operation(_entries[a.entry], _entries[b.entry]);
  });
  _remap.assign(n + 1, 0);
  _sorted.resize(n);
  for (uint32_t pos = 0; pos < n; ++pos) {
    _remap[_order[pos].entry + 1] = pos + 1;
    _sorted[pos] = _entries[_order[pos].entry];
  }
  _entries.swap(_sorted);
  for (auto& code : _codes) {
    code = _remap[code];
  }
}

void SegmentBuilder::WriteSegment(const EncodedFrames& frames,
                                  std::string& data, Segment& out) {
  const auto entry_count = EntryCount();
  SDB_ENSURE(_rows <= std::numeric_limits<uint32_t>::max() &&
               entry_count <= std::numeric_limits<uint32_t>::max() &&
               data.size() <= std::numeric_limits<uint32_t>::max(),
             "col codec: segment too large");

  Header h{
    .shape = _shape,
    .codec = frames.codec,
    .level = _level,
    .length_width =
      BitpackingPrimitives::MinimumBitWidth<uint32_t>(frames.max_length),
    .row_count = static_cast<uint32_t>(_rows),
    .entry_count = static_cast<uint32_t>(entry_count),
    .frame_count = static_cast<uint32_t>(frames.frames.size()),
    .dictionary_size = frames.dictionary_size,
    .symtab_size = static_cast<uint32_t>(frames.symtab.size()),
    .data_size = static_cast<uint32_t>(data.size()),
    .lcp_width =
      BitpackingPrimitives::MinimumBitWidth<uint32_t>(frames.max_lcp),
    .flags = frames.flags,
    .raw_bytes = _raw,
  };
  const auto codes =
    _shape == Shape::Dedup
      ? EncodeCodes(_codes, _entries.size(), _runs, _codes_tuning)
      : std::string{};
  LayOut(h, frames.frames.size(),
         BitpackingPrimitives::GetRequiredSize(entry_count, h.length_width),
         BitpackingPrimitives::GetRequiredSize(entry_count, h.lcp_width),
         codes.size(), frames.symtab.size());

  out.head.assign(static_cast<size_t>(h.off_data), '\0');
  auto* base = reinterpret_cast<duckdb::data_ptr_t>(out.head.data());
  duckdb::Store(h, base);
  for (size_t i = 0; i < frames.frames.size(); ++i) {
    duckdb::Store(frames.frames[i], base + h.off_frames + i * kFrameMetaSize);
  }
  BitpackingPrimitives::PackBuffer<uint32_t, true>(
    base + h.off_lengths, _lengths.data(), _lengths.size(), h.length_width);
  if (!frames.lcps.empty()) {
    BitpackingPrimitives::PackBuffer<uint32_t, true>(
      base + h.off_lcps, frames.lcps.data(), frames.lcps.size(), h.lcp_width);
  }
  if (!codes.empty()) {
    std::memcpy(base + h.off_codes, codes.data(), codes.size());
  }
  if (!frames.symtab.empty()) {
    std::memcpy(base + h.off_symtab, frames.symtab.data(),
                frames.symtab.size());
  }

  _stats.Merge(out.stats.emplace(duckdb::BaseStatistics::CreateEmpty(_type)));
  out.config = {
    .choice = {_shape, frames.codec},
    .level = _level,
    .layout = _layout,
  };
  out.begin = _next - _rows;
  out.rows = _rows;
  out.entries = _entries.size();
  out.nulls = _nulls;
  out.input = _input;
  out.max_len = _max_len;
  out.data.swap(data);

  _stats.Clear();
  _codes.clear();
  _entries.clear();
  _rows = 0;
  _runs = 0;
  _raw = 0;
  _max_len = 0;
  _nulls = 0;
  _input = 0;
}

}  // namespace irs::codecs::text
