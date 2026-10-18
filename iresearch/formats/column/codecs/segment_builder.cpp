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

#include <absl/base/internal/endian.h>
#include <absl/strings/match.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <limits>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text {
namespace {

using duckdb::BitpackingPrimitives;
using duckdb::string_t;

constexpr double kMispredicted = 2.0;

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

CodesPlan PlanCodes(Shape shape, uint64_t rows, uint64_t entries,
                    uint64_t runs) noexcept {
  CodesPlan plan;
  if (shape != Shape::Dedup) {
    return plan;
  }
  const auto max_code = static_cast<uint32_t>(entries);
  const auto bitpack = Packed(rows, max_code);
  const auto rle =
    Packed(runs, max_code) + Packed(runs, static_cast<uint32_t>(rows));
  plan.code_width = BitpackingPrimitives::MinimumBitWidth<uint32_t>(max_code);
  if (rle < bitpack) {
    plan.encoding = CodesEncoding::Rle;
    plan.run_width = BitpackingPrimitives::MinimumBitWidth<uint32_t>(
      static_cast<uint32_t>(rows));
    plan.codes_count = runs;
    plan.run_count = runs;
    plan.bytes = rle;
  } else {
    plan.codes_count = rows;
    plan.bytes = bitpack;
  }
  return plan;
}

void LayOut(Header& h, uint64_t frames, uint64_t lengths_bytes,
            uint64_t lcps_bytes, const CodesPlan& codes,
            uint64_t symtab_bytes) noexcept {
  h.off_frames = kHeaderSize;
  h.off_lengths = Align8(kHeaderSize + frames * kFrameMetaSize);
  h.off_lcps = Align8(h.off_lengths + lengths_bytes);
  h.off_codes = Align8(h.off_lcps + lcps_bytes);
  h.off_runs = Align8(h.off_codes + BitpackingPrimitives::GetRequiredSize(
                                      codes.codes_count, codes.code_width));
  h.off_symtab = Align8(h.off_runs + BitpackingPrimitives::GetRequiredSize(
                                       codes.run_count, codes.run_width));
  h.off_data = Align8(h.off_symtab + symtab_bytes);
}

void Record(RatioHistory& hist, uint64_t raw, uint64_t comp,
            uint32_t target) noexcept {
  if (hist.raw != 0 && raw != 0 && comp * kDriftMinFraction >= target) {
    const auto predicted = static_cast<double>(raw) *
                           static_cast<double>(hist.comp) /
                           static_cast<double>(hist.raw);
    const auto actual = static_cast<double>(comp);
    if (actual > predicted * kMispredicted ||
        actual * kMispredicted < predicted) {
      hist = {};
    }
  }
  hist.raw += raw;
  hist.comp += comp;
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
         PlanCodes(_shape, _rows, _entries.size(), _runs).bytes;
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
  std::sort(_order.begin(), _order.end(),
            [&](const std::pair<uint64_t, uint32_t>& a,
                const std::pair<uint64_t, uint32_t>& b) {
              if (a.first != b.first) {
                return a.first < b.first;
              }
              return _entries[a.second] < _entries[b.second];
            });
  _remap.assign(n + 1, 0);
  _sorted.resize(n);
  for (uint32_t pos = 0; pos < n; ++pos) {
    _remap[_order[pos].second + 1] = pos + 1;
    _sorted[pos] = _entries[_order[pos].second];
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
  h.shape = static_cast<uint8_t>(_shape);
  h.codec = static_cast<uint8_t>(frames.codec);
  h.level = _level;
  h.length_width =
    BitpackingPrimitives::MinimumBitWidth<uint32_t>(frames.max_length);
  h.lcp_width = BitpackingPrimitives::MinimumBitWidth<uint32_t>(frames.max_lcp);
  const auto codes = PlanCodes(_shape, _rows, _entries.size(), _runs);
  h.code_width = codes.code_width;
  h.row_count = static_cast<uint32_t>(_rows);
  h.entry_count = static_cast<uint32_t>(entry_count);
  h.frame_count = static_cast<uint32_t>(frames.frames.size());
  h.flags = frames.flags;
  h.dictionary = frames.dictionary;
  h.raw_bytes = _raw;

  if (codes.encoding == CodesEncoding::Rle) {
    _run_values.clear();
    _run_ends.clear();
    for (size_t i = 0; i < _codes.size(); ++i) {
      if (i != 0 && _codes[i] == _codes[i - 1]) {
        continue;
      }
      if (i != 0) {
        _run_ends.push_back(static_cast<uint32_t>(i));
      }
      _run_values.push_back(_codes[i]);
    }
    _run_ends.push_back(static_cast<uint32_t>(_codes.size()));
    SDB_ASSERT(_run_values.size() == codes.run_count);
  }
  h.codes_encoding = static_cast<uint8_t>(codes.encoding);
  h.run_count = static_cast<uint32_t>(codes.run_count);
  h.run_width = codes.run_width;

  LayOut(h, frames.frames.size(),
         BitpackingPrimitives::GetRequiredSize(entry_count, h.length_width),
         frames.lcps.empty()
           ? 0
           : BitpackingPrimitives::GetRequiredSize(entry_count, h.lcp_width),
         codes, frames.symtab.size());
  h.symtab_size = static_cast<uint32_t>(frames.symtab.size());
  h.data_size = static_cast<uint32_t>(data.size());

  _bytes.assign(static_cast<size_t>(h.off_data), '\0');
  auto* base = reinterpret_cast<duckdb::data_ptr_t>(_bytes.data());
  h.Write(base);
  for (size_t i = 0; i < frames.frames.size(); ++i) {
    frames.frames[i].Store(base + h.off_frames + i * kFrameMetaSize);
  }
  BitpackingPrimitives::PackBuffer<uint32_t, true>(
    base + h.off_lengths, _lengths.data(), _lengths.size(), h.length_width);
  if (!frames.lcps.empty()) {
    BitpackingPrimitives::PackBuffer<uint32_t, true>(
      base + h.off_lcps, frames.lcps.data(), frames.lcps.size(), h.lcp_width);
  }
  if (_shape == Shape::Dedup) {
    if (codes.encoding == CodesEncoding::Rle) {
      _run_values.resize(GroupPadded(_run_values.size()), 0);
      _run_ends.resize(GroupPadded(_run_ends.size()), 0);
      BitpackingPrimitives::PackBuffer<uint32_t, true>(
        base + h.off_codes, _run_values.data(), _run_values.size(),
        h.code_width);
      BitpackingPrimitives::PackBuffer<uint32_t, true>(
        base + h.off_runs, _run_ends.data(), _run_ends.size(), h.run_width);
    } else {
      _codes.resize(GroupPadded(_rows), 0);
      BitpackingPrimitives::PackBuffer<uint32_t, true>(
        base + h.off_codes, _codes.data(), _codes.size(), h.code_width);
      _codes.resize(_rows);
    }
  }
  if (!frames.symtab.empty()) {
    std::memcpy(base + h.off_symtab, frames.symtab.data(),
                frames.symtab.size());
  }

  auto stats = duckdb::BaseStatistics::CreateEmpty(_type);
  _stats.Merge(stats);
  out.choice = StringChoice{_shape, frames.codec};
  out.level = _level;
  out.layout = _layout;
  out.begin = _next - _rows;
  out.rows = _rows;
  out.entries = _entries.size();
  out.nulls = _nulls;
  out.input = _input;
  out.max_len = _max_len;
  out.stats.emplace(std::move(stats));
  out.head.swap(_bytes);
  out.data.swap(data);
  out.codes.swap(_codes);

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
