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
#include <limits>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text {
namespace {

using duckdb::BitpackingPrimitives;
using duckdb::string_t;

uint64_t PrefixKey(std::string_view sv, size_t skip) noexcept {
  char buf[8] = {};
  if (sv.size() > skip) {
    std::memcpy(buf, sv.data() + skip,
                std::min<size_t>(sv.size() - skip, sizeof buf));
  }
  return absl::big_endian::Load64(buf);
}

}  // namespace

uint64_t Packed(uint64_t count, uint32_t max_value) noexcept {
  return BitpackingPrimitives::GetRequiredSize(
    count, BitpackingPrimitives::MinimumBitWidth<uint32_t>(max_value));
}

uint64_t CodesBytes(Shape shape, uint64_t rows, uint64_t entries,
                    uint64_t runs) noexcept {
  if (shape != Shape::Dedup) {
    return 0;
  }
  const auto max_code = static_cast<uint32_t>(entries);
  return std::min(
    Packed(rows, max_code),
    Packed(runs, max_code) + Packed(runs, static_cast<uint32_t>(rows)));
}

void LayOut(Header& h, uint64_t frames, uint64_t lengths_bytes,
            uint64_t lcps_bytes, uint64_t codes_bytes,
            uint64_t symtab_bytes) noexcept {
  h.off_frames = kHeaderSize;
  h.off_lengths =
    duckdb::AlignValue<uint64_t, 8>(kHeaderSize + frames * kFrameMetaSize);
  h.off_lcps = duckdb::AlignValue<uint64_t, 8>(h.off_lengths + lengths_bytes);
  h.off_codes = duckdb::AlignValue<uint64_t, 8>(h.off_lcps + lcps_bytes);
  h.off_symtab = duckdb::AlignValue<uint64_t, 8>(h.off_codes + codes_bytes);
  h.off_data = duckdb::AlignValue<uint64_t, 8>(h.off_symtab + symtab_bytes);
}

void SegmentBuilder::Begin(Shape shape, uint8_t level, FrameLayout layout,
                           uint64_t begin) {
  _shape = shape;
  _level = level;
  _layout = layout;
  _next = begin;
  if (shape == Shape::Dedup) {
    _dedup.Begin(_acc.entries.size());
  }
}

uint64_t SegmentBuilder::Estimate() const noexcept {
  const auto entry_count = _entries.size() + FirstEntry();
  return kHeaderSize + Packed(entry_count, _max_len) +
         CodesBytes(_shape, _rows, _entries.size(), _runs);
}

void SegmentBuilder::SortEntries() {
  const auto n = static_cast<uint32_t>(_entries.size());
  size_t common = _entries.empty() ? 0 : _entries[0].size();
  for (uint32_t i = 1; i < n && common != 0; ++i) {
    common = std::min(
      common, absl::FindLongestCommonPrefix(_entries[0], _entries[i]).size());
  }
  _order.resize(n);
  for (uint32_t i = 0; i < n; ++i) {
    _order[i] = {PrefixKey(_entries[i], common), i};
  }
  absl::c_sort(_order, [&](const SortKey& a, const SortKey& b) {
    if (a.prefix != b.prefix) {
      return a.prefix < b.prefix;
    }
    return _entries[a.entry] < _entries[b.entry];
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
  const auto entry_count = _entries.size() + FirstEntry();
  SDB_ENSURE(_rows <= std::numeric_limits<uint32_t>::max() &&
               entry_count <= std::numeric_limits<uint32_t>::max() &&
               data.size() <= std::numeric_limits<uint32_t>::max(),
             "col codec: segment too large");

  Header h;
  h.shape = _shape;
  h.codec = frames.codec;
  h.level = _level;
  h.length_width =
    BitpackingPrimitives::MinimumBitWidth<uint32_t>(frames.max_length);
  h.lcp_width = BitpackingPrimitives::MinimumBitWidth<uint32_t>(frames.max_lcp);
  h.row_count = static_cast<uint32_t>(_rows);
  h.entry_count = static_cast<uint32_t>(entry_count);
  h.frame_count = static_cast<uint32_t>(frames.frames.size());
  h.flags = frames.flags;
  h.dictionary = frames.dictionary;
  h.raw_bytes = _raw;

  std::string codes;
  if (_shape == Shape::Dedup) {
    codes = EncodeCodes(_codes, _codes_tuning);
  }
  LayOut(h, frames.frames.size(),
         BitpackingPrimitives::GetRequiredSize(entry_count, h.length_width),
         frames.lcps.empty()
           ? 0
           : BitpackingPrimitives::GetRequiredSize(entry_count, h.lcp_width),
         codes.size(), frames.symtab.size());
  h.symtab_size = static_cast<uint32_t>(frames.symtab.size());
  h.data_size = static_cast<uint32_t>(data.size());

  _bytes.assign(static_cast<size_t>(h.off_data), '\0');
  auto* base = reinterpret_cast<duckdb::data_ptr_t>(_bytes.data());
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

  auto stats = duckdb::BaseStatistics::CreateEmpty(_type);
  _stats.Merge(stats);
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
  out.stats.emplace(std::move(stats));
  out.head.swap(_bytes);
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
