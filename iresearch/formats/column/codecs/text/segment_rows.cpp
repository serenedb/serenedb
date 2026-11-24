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

#include "iresearch/formats/column/codecs/text/segment_rows.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/helper.hpp>
#include <limits>
#include <span>
#include <string_view>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text {
namespace {

using duckdb::BitpackingPrimitives;
using duckdb::string_t;

constexpr double kEntriesFirstRatio = 0.5;
constexpr double kMispredicted = 2.0;
constexpr uint32_t kPackedCodeRunRows = 4;
constexpr double kCodeRunPrice = 1.5;

uint64_t PrefixKey(const string_t& value, size_t skip) noexcept {
  char buf[8] = {};
  const size_t size = value.GetSize();
  if (size > skip) {
    std::memcpy(buf, value.GetData() + skip,
                std::min<size_t>(size - skip, sizeof buf));
  }
  return absl::big_endian::Load64(buf);
}

void SortByBytes(std::span<SortScratch::Key> order,
                 std::span<const string_t> entries, size_t skip) {
  absl::c_sort(
    order, [](const auto& a, const auto& b) { return a.prefix < b.prefix; });
  const size_t next = skip + sizeof(uint64_t);
  for (size_t first = 0; first < order.size();) {
    auto end = first + 1;
    while (end < order.size() && order[end].prefix == order[first].prefix) {
      ++end;
    }
    const auto tied = order.subspan(first, end - first);
    first = end;
    if (tied.size() == 1) {
      continue;
    }
    const auto longer = absl::c_partition(tied, [&](const auto& key) {
      return entries[key.entry].GetSize() <= next;
    });
    const std::span ended{tied.begin(), longer};
    absl::c_sort(ended, [&](const auto& a, const auto& b) {
      return entries[a.entry].GetSize() < entries[b.entry].GetSize();
    });
    const std::span rest{longer, tied.end()};
    if (rest.empty()) {
      continue;
    }
    const auto tail = [&](const SortScratch::Key& key) {
      const auto& entry = entries[key.entry];
      return std::string_view{entry.GetData() + next, entry.GetSize() - next};
    };
    auto shared = tail(rest.front());
    for (const auto& key : rest.subspan(1)) {
      shared = absl::FindLongestCommonPrefix(shared, tail(key));
    }
    const auto deeper = next + shared.size();
    for (auto& key : rest) {
      key.prefix = PrefixKey(entries[key.entry], deeper);
    }
    SortByBytes(rest, entries, deeper);
  }
}

}  // namespace

uint64_t CodesBytes(uint64_t rows, uint64_t entries, double ratio) noexcept {
  return ratio != 0 ? static_cast<uint64_t>(
                        static_cast<double>(rows * sizeof(uint32_t)) * ratio)
                    : Packed(rows, entries);
}

void CompressCodes(
  std::span<uint32_t> codes, double& ratio,
  absl::FunctionRef<void(CodecType codec, std::string_view bytes)> write) {
  SDB_ASSERT(!codes.empty());
  const WriteChunk chunk{
    duckdb::Vector{duckdb::LogicalType::UINTEGER,
                   reinterpret_cast<duckdb::data_ptr_t>(codes.data()),
                   codes.size()},
    codes.size()};
  ColumnTuning column;
  uint64_t written = 0;
  const auto flush = [&](const Codec& codec, duckdb::BaseStatistics, uint64_t,
                         std::string_view bytes, std::string_view) {
    SDB_ASSERT(CodesCodec(codec.type));
    written += bytes.size();
    write(codec.type, bytes);
  };
  ColumnDataCheckpointData data{
    .type = chunk.data.GetType(),
    .chunks = {&chunk, 1},
    .flush = flush,
    .tuning = column,
    .disabled = ~kCodesGroup,
    .min_packed_run = kPackedCodeRunRows,
    .run_price = kCodeRunPrice,
  };
  Compress(CodecType::Auto, data);
  ratio = static_cast<double>(written) /
          static_cast<double>(codes.size() * sizeof(uint32_t));
}

uint64_t EstimateEntries(const SegmentEntries& rows,
                         const RatioHistory& hist) noexcept {
  return Packed(EntryCount(rows), rows.max_len) +
         (rows.raw / kFsstFrameRawBytes + 1) * kFrameMetaSize +
         static_cast<uint64_t>(static_cast<double>(rows.raw) *
                               Ratio(hist, kEntriesFirstRatio));
}

void RecordRatio(RatioHistory& hist, uint64_t raw, uint64_t comp,
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

uint64_t PlainRows::Estimate() const noexcept {
  return kHeaderSize + Packed(EntryCount(), _max_len);
}

uint64_t DictionaryRows::Estimate() const noexcept {
  return kHeaderSize + Packed(EntryCount(), _max_len) +
         CodesBytes(_rows, _entries.size(), _codes_ratio);
}

void SortEntries(std::span<const string_t> entries, SortScratch& scratch) {
  const auto n = static_cast<uint32_t>(entries.size());
  uint32_t common = entries.empty() ? 0 : entries[0].GetSize();
  for (uint32_t i = 1; i < n && common != 0; ++i) {
    common = std::min(common, CommonPrefix(entries[0], entries[i]));
  }
  auto& order = scratch.order;
  order.resize(n);
  for (uint32_t i = 0; i < n; ++i) {
    order[i] = {PrefixKey(entries[i], common), i};
  }
  SortByBytes(order, entries, common);
  auto& remap = scratch.remap;
  auto& sorted = scratch.sorted;
  remap.assign(n + 1, 0);
  sorted.resize(n);
  for (uint32_t pos = 0; pos < n; ++pos) {
    remap[order[pos].entry + 1] = pos + 1;
    sorted[pos] = entries[order[pos].entry];
  }
}

void DictionaryRows::Sort(SortScratch& scratch) {
  SortEntries(_entries, scratch);
  _entries.swap(scratch.sorted);
  SDB_ASSERT((absl::c_adjacent_find(
                _entries, [](const string_t& left, const string_t& right) {
                  return std::string_view{right.GetData(), right.GetSize()} <=
                         std::string_view{left.GetData(), left.GetSize()};
                }) == _entries.end()));
  for (auto& code : _codes) {
    code = scratch.remap[code];
  }
  _sorted = true;
}

void SegmentRows::Assemble(const EncodedFrames& frames, CodecType codes_codec,
                           std::string_view codes, bool sorted, Segment& out) {
  const auto entry_count = EntryCount();
  const auto data_size = out.bytes.Size();
  SDB_ENSURE(_rows <= std::numeric_limits<uint32_t>::max() &&
               entry_count <= std::numeric_limits<uint32_t>::max() &&
               data_size <= std::numeric_limits<uint32_t>::max(),
             "col codec: segment too large");
  Header h{
    .codes = codes_codec,
    .level = _level,
    .sorted = sorted,
    .length_width =
      BitpackingPrimitives::MinimumBitWidth<uint32_t>(frames.max_length),
    .row_count = static_cast<uint32_t>(_rows),
    .entry_count = static_cast<uint32_t>(entry_count),
    .frame_count = static_cast<uint32_t>(frames.frames.size()),
    .dictionary_size = frames.dictionary_size,
    .symtab_size = static_cast<uint32_t>(frames.symtab.size()),
    .data_size = static_cast<uint32_t>(data_size),
    .lcp_width =
      BitpackingPrimitives::MinimumBitWidth<uint32_t>(frames.max_lcp),
    .flags = frames.flags,
    .raw_bytes = _raw,
  };
  const auto total =
    LayOut(h, data_size, frames.frames.size(),
           BitpackingPrimitives::GetRequiredSize(entry_count, h.length_width),
           BitpackingPrimitives::GetRequiredSize(entry_count, h.lcp_width),
           codes.size(), frames.symtab.size());

  out.bytes.Extend(total - data_size);
  auto* base = reinterpret_cast<duckdb::data_ptr_t>(out.bytes.Data());
  std::memmove(base + h.off_data, base, data_size);
  std::memset(base, 0, h.off_data);
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
}

void SegmentRows::Finish(const EncodedFrames& frames, Shape shape,
                         Segment& out) {
  out.stats = duckdb::BaseStatistics::CreateEmpty(_type);
  _stats.Merge(out.stats);
  out.config = {
    .shape = shape,
    .leaf = frames.codec,
    .level = _level,
    .layout = _layout,
    .sorted = _ordered,
  };
  out.begin = _next - _rows;
  out.rows = _rows;
  out.entries = _entries.size();
  out.nulls = _nulls;
  out.input = _input;
  out.max_len = _max_len;
  out.shared = frames.shared;

  _stats.Clear();
  _entries.clear();
  _rows = 0;
  _raw = 0;
  _max_len = 0;
  _nulls = 0;
  _input = 0;
}

void PlainRows::Write(const EncodedFrames& frames, Segment& out) {
  Assemble(frames, CodecType::Auto, {}, false, out);
  Finish(frames, kShape, out);
}

void DictionaryRows::Write(const EncodedFrames& frames, Segment& out) {
  CompressCodes(_codes, _codes_ratio,
                [&](CodecType codec, std::string_view codes) {
                  Assemble(frames, codec, codes, _sorted, out);
                });
  Finish(frames, kShape, out);
  _codes.clear();
  _sorted = false;
}

}  // namespace irs::codecs::text
