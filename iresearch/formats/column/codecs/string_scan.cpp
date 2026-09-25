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

#include "iresearch/formats/column/codecs/string_scan.hpp"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <duckdb/storage/segment/uncompressed.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/dictionary_cache.hpp"
#include "iresearch/formats/column/codecs/fsst_codec.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs::codecs {
namespace {

using duckdb::BitpackingPrimitives;
using duckdb::const_data_ptr_t;
using duckdb::data_ptr_t;
using duckdb::idx_t;
using duckdb::string_t;

constexpr size_t kFsstMaxExpansion = 8;

std::vector<uint32_t> Unpack(data_ptr_t src, idx_t count, uint8_t width) {
  std::vector<uint32_t> out(GroupPadded(count));
  if (count != 0) {
    BitpackingPrimitives::UnPackBuffer<uint32_t>(
      duckdb::data_ptr_cast(out.data()), src, count, width);
  }
  out.resize(count);
  return out;
}

struct PackedReader {
  PackedReader(data_ptr_t src, uint8_t width) : src{src}, width{width} {}

  uint32_t At(idx_t i) {
    const idx_t group = i / kGroup;
    if (group != loaded) {
      BitpackingPrimitives::UnPackBlock<uint32_t>(
        duckdb::data_ptr_cast(values), src + (group * kGroup * width) / 8,
        width);
      loaded = group;
    }
    return values[i % kGroup];
  }

  data_ptr_t src;
  uint8_t width;
  idx_t loaded = std::numeric_limits<idx_t>::max();
  uint32_t values[kGroup];
};

const ReadContext::CacheSlot& CacheSlotOf(
  duckdb::ColumnSegment& segment) noexcept {
  static const ReadContext::CacheSlot kNone;
  const auto& block = segment.GetBlockHandle();
  if (!block) {
    return kNone;
  }
  return static_cast<const ReadContext&>(block->GetBlockManager())
    .CacheSlotOf(block->BlockId());
}

using EntryRanges = std::vector<std::pair<uint32_t, uint32_t>>;

EntryRanges Normalize(EntryRanges ranges) {
  std::sort(ranges.begin(), ranges.end());
  EntryRanges out;
  for (const auto& [lo, hi] : ranges) {
    if (lo >= hi) {
      continue;
    }
    if (!out.empty() && lo <= out.back().second) {
      out.back().second = std::max(out.back().second, hi);
    } else {
      out.emplace_back(lo, hi);
    }
  }
  return out;
}

EntryRanges Intersect(const EntryRanges& a, const EntryRanges& b) {
  EntryRanges out;
  size_t i = 0;
  size_t j = 0;
  while (i < a.size() && j < b.size()) {
    const auto lo = std::max(a[i].first, b[j].first);
    const auto hi = std::min(a[i].second, b[j].second);
    if (lo < hi) {
      out.emplace_back(lo, hi);
    }
    if (a[i].second < b[j].second) {
      ++i;
    } else {
      ++j;
    }
  }
  return out;
}

bool IsColumn(const duckdb::Expression& expr) noexcept {
  return expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF &&
         expr.GetReturnType().id() == duckdb::LogicalTypeId::VARCHAR;
}

const duckdb::Value* StringConstant(const duckdb::Expression& expr) noexcept {
  if (expr.GetExpressionType() != duckdb::ExpressionType::VALUE_CONSTANT) {
    return nullptr;
  }
  const auto& value = expr.Cast<duckdb::BoundConstantExpression>().GetValue();
  if (value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return nullptr;
  }
  return &value;
}

std::string_view View(const string_t& s) noexcept {
  return {s.GetData(), s.GetSize()};
}

constexpr uint32_t kNoFrame = std::numeric_limits<uint32_t>::max();
constexpr idx_t kSparseEntries = 64;

struct ScanState final : duckdb::SegmentScanState {
  ScanState(duckdb::ColumnSegment& segment, duckdb::BufferHandle handle_p)
    : handle{std::move(handle_p)},
      base{handle.GetDataMutable() + segment.GetBlockOffset()},
      header{Header::Parse(base, segment.SegmentSize())},
      type{segment.GetType()},
      length_reader{base + header.off_lengths, header.length_width},
      lcp_reader{base + header.off_lcps, header.lcp_width} {
    const auto& slot = CacheSlotOf(segment);
    if (Dedup() && !slot.key.empty()) {
      cache = &segment.GetDatabase().GetObjectCache();
      cache_key = slot.key;
      touched = slot.touched;
      if (const auto cached = cache->Get<DecodedDictionary>(cache_key)) {
        AdoptDictionary(*cached);
        LoadCodes();
        return;
      }
    }
    dictionary = duckdb::DictionaryVector::CreateReusableDictionary(
      type, header.entry_count);
    auto& dict_data = dictionary->data;
    values = duckdb::FlatVector::GetDataMutable<string_t>(dict_data);
    const auto heap_bytes =
      std::max<idx_t>(header.raw_bytes, string_t::INLINE_LENGTH + 1);
    heap = duckdb::StringVector::EmptyString(dict_data, heap_bytes)
             .GetDataWriteable();
    LoadFrames();
    switch (static_cast<ByteCodec>(header.codec)) {
      case ByteCodec::Lz4:
        lz4.emplace();
        break;
      case ByteCodec::Zstd:
        zstd.emplace();
        break;
      case ByteCodec::Zxc:
        zxc.emplace();
        break;
      case ByteCodec::Fsst:
        fsst.emplace();
        SDB_ENSURE(header.symtab_size == 0 ||
                     fsst->Import(std::string_view{
                       reinterpret_cast<const char*>(base + header.off_symtab),
                       header.symtab_size}),
                   "col codec: corrupted symbol table");
        LoadGroups();
        break;
    }
    if (!Dedup()) {
      return;
    }
    values[0] = string_t{};
    duckdb::FlatVector::ValidityMutable(dict_data).SetInvalid(0);
    full = frames.empty();
    LoadCodes();
  }

  void AdoptDictionary(const DecodedDictionary& cached) {
    const auto& owner = cached.Dictionary();
    duckdb::Vector view{type, header.entry_count};
    std::memcpy(duckdb::FlatVector::GetDataMutable<string_t>(view),
                duckdb::FlatVector::GetData<string_t>(owner->data),
                static_cast<size_t>(header.entry_count) * sizeof(string_t));
    duckdb::FlatVector::SetSize(view, header.entry_count);
    duckdb::FlatVector::ValidityMutable(view).SetInvalid(0);
    duckdb::StringVector::AddAuxiliaryData(
      view, duckdb::make_uniq<duckdb::VectorBufferHolder>(
              owner->data.GetBufferRef()));
    dictionary = duckdb::make_buffer<duckdb::DictionaryEntry>(std::move(view));
    dictionary->id = owner->id;
    values = duckdb::FlatVector::GetDataMutable<string_t>(dictionary->data);
    full = true;
  }

  void EnsureDictionary() {
    if (full) {
      return;
    }
    if (cache) {
      if (const auto cached = cache->Get<DecodedDictionary>(cache_key)) {
        AdoptDictionary(*cached);
        return;
      }
    }
    DecodeAll();
  }

  void Admit() {
    full = true;
    if (!cache || !touched->exchange(true, std::memory_order_relaxed)) {
      return;
    }
    const auto bytes = CacheBytes();
    if (cache->GetCurrentMemory() + bytes <= cache->GetMaxMemory()) {
      cache->Put(cache_key,
                 duckdb::make_shared_ptr<DecodedDictionary>(dictionary, bytes));
    }
  }

  uint64_t CacheBytes() const noexcept {
    return header.raw_bytes +
           static_cast<uint64_t>(header.entry_count) * (sizeof(string_t) + 1);
  }

  void LoadCodes() {
    if (!Rle()) {
      return;
    }
    run_values =
      Unpack(base + header.off_codes, header.run_count, header.code_width);
    run_ends =
      Unpack(base + header.off_runs, header.run_count, header.run_width);
    SDB_ENSURE(!run_ends.empty() && run_ends.back() == header.row_count,
               "col codec: corrupted run ends");
  }

  bool Dedup() const noexcept {
    return header.shape == static_cast<uint8_t>(Shape::Dedup);
  }
  bool Rle() const noexcept {
    return header.codes_encoding == static_cast<uint8_t>(CodesEncoding::Rle);
  }
  bool Sorted() const noexcept {
    return Dedup() && header.codec == static_cast<uint8_t>(ByteCodec::Fsst);
  }
  uint32_t FirstEntry() const noexcept { return Dedup() ? 1 : 0; }

  void LoadFrames() {
    frames.resize(header.frame_count);
    frame_off.resize(header.frame_count + 1);
    decoded.assign(header.frame_count, 0);
    uint64_t raw = 0;
    for (uint32_t i = 0; i < header.frame_count; ++i) {
      const auto f =
        FrameMeta::Load(base + header.off_frames + i * kFrameMetaSize);
      SDB_ENSURE(
        (i == 0 ? f.first_entry == FirstEntry()
                : f.first_entry > frames[i - 1].first_entry) &&
          f.first_entry < header.entry_count &&
          static_cast<uint64_t>(f.comp_off) + f.comp_len <= header.data_size &&
          raw + f.raw_len <= header.raw_bytes,
        "col codec: corrupted frame table");
      frames[i] = f;
      frame_off[i] = raw;
      raw += f.raw_len;
    }
    frame_off[header.frame_count] = raw;
    SDB_ENSURE(raw == header.raw_bytes && (header.frame_count != 0 ||
                                           header.entry_count == FirstEntry()),
               "col codec: corrupted frame table");
  }

  void LoadGroups() {
    group_base.resize(frames.size() + 1);
    uint32_t groups = 0;
    for (uint32_t f = 0; f < frames.size(); ++f) {
      group_base[f] = groups;
      groups += (FrameEnd(f) - frames[f].first_entry + kChainRestart - 1) /
                kChainRestart;
    }
    group_base[frames.size()] = groups;
    group_decoded.assign(groups, 0);
    group_enc_off.resize(groups);
    group_offsets_ready.assign(frames.size(), 0);
  }

  uint32_t FrameEnd(uint32_t f) const noexcept {
    return f + 1 < frames.size() ? frames[f + 1].first_entry
                                 : header.entry_count;
  }

  uint32_t FrameOf(uint32_t entry) const noexcept {
    const auto it = std::upper_bound(
      frames.begin(), frames.end(), entry,
      [](uint32_t e, const FrameMeta& m) { return e < m.first_entry; });
    SDB_ASSERT(it != frames.begin());
    return static_cast<uint32_t>(it - frames.begin()) - 1;
  }

  void DecodeAll() {
    for (uint32_t f = 0; f < frames.size(); ++f) {
      if (!decoded[f]) {
        DecodeFrame(f);
      }
    }
  }

  void EnsureEntry(uint32_t e, uint32_t& f, bool sparse) {
    if (f == kNoFrame || e < frames[f].first_entry || e >= FrameEnd(f)) {
      f = FrameOf(e);
    }
    if (decoded[f]) {
      return;
    }
    if (!sparse || !fsst) {
      DecodeFrame(f);
      return;
    }
    const auto g = (e - frames[f].first_entry) / kChainRestart;
    if (!group_decoded[group_base[f] + g]) {
      DecodeGroup(f, g);
    }
  }

  void EnsureEntries(uint32_t first, uint32_t last) {
    if (decoded_count == frames.size()) {
      return;
    }
    const bool sparse = last - first < kSparseEntries;
    uint32_t f = kNoFrame;
    for (auto e = first; e <= last; ++e) {
      EnsureEntry(e, f, sparse);
    }
  }

  void EnsureSelected(idx_t start, const duckdb::SelectionVector& rows,
                      idx_t count) {
    if (decoded_count == frames.size() || count == 0) {
      return;
    }
    const bool sparse = count <= kSparseEntries;
    uint32_t f = kNoFrame;
    for (idx_t i = 0; i < count; ++i) {
      EnsureEntry(static_cast<uint32_t>(start + rows.get_index(i)), f, sparse);
    }
  }

  void EnsureCodes(const duckdb::SelectionVector& codes, idx_t count) {
    const bool sparse = count <= kSparseEntries;
    uint32_t f = kNoFrame;
    for (idx_t i = 0; i < count; ++i) {
      if (const auto code = codes.get_index(i); code != 0) {
        EnsureEntry(static_cast<uint32_t>(code), f, sparse);
      }
    }
  }

  void DecodeFrame(uint32_t f) {
    SDB_ASSERT(!decoded[f]);
    const auto& m = frames[f];
    const auto end = FrameEnd(f);
    char* out = heap + frame_off[f];
    switch (static_cast<ByteCodec>(header.codec)) {
      case ByteCodec::Lz4:
        DecodeBlobFrame(*lz4, m, end, out);
        break;
      case ByteCodec::Zstd:
        DecodeBlobFrame(*zstd, m, end, out);
        break;
      case ByteCodec::Zxc:
        DecodeBlobFrame(*zxc, m, end, out);
        break;
      case ByteCodec::Fsst:
        DecodeFsstFrame(m, end, out);
        break;
    }
    decoded[f] = 1;
    if (++decoded_count == frames.size() && Dedup()) {
      Admit();
    }
  }

  template<typename Decompressor>
  void DecodeBlobFrame(Decompressor& d, const FrameMeta& m, uint32_t end,
                       char* out) {
    SDB_ENSURE(
      d.Decompress(
        reinterpret_cast<const char*>(base + header.off_data) + m.comp_off,
        m.comp_len, out, m.raw_len),
      "col codec: corrupted frame");
    uint64_t off = 0;
    for (uint32_t e = m.first_entry; e < end; ++e) {
      const auto len = length_reader.At(e);
      SDB_ENSURE(off + len <= m.raw_len, "col codec: corrupted entry lengths");
      values[e] = string_t{out + off, len};
      off += len;
    }
    SDB_ENSURE(off == m.raw_len, "col codec: corrupted entry lengths");
  }

  void DecodeFsstFrame(const FrameMeta& m, uint32_t end, char* out) {
    const auto* in = reinterpret_cast<const char*>(base + header.off_data);
    uint64_t enc_off = m.comp_off;
    const uint64_t enc_end = static_cast<uint64_t>(m.comp_off) + m.comp_len;
    uint64_t off = 0;
    uint64_t prev_off = 0;
    uint64_t prev_len = 0;
    for (uint32_t e = m.first_entry; e < end; ++e) {
      const uint64_t lcp = lcp_reader.At(e);
      const uint64_t enc = length_reader.At(e);
      SDB_ENSURE(
        lcp <= prev_len && enc_off + enc <= enc_end && off + lcp <= m.raw_len,
        "col codec: corrupted front coding");
      std::memcpy(out + off, out + prev_off, lcp);
      const auto capacity = m.raw_len - (off + lcp);
      const auto dec =
        enc == 0 ? 0
                 : fsst->Decode(in + enc_off, enc, out + off + lcp, capacity);
      SDB_ENSURE(dec <= capacity, "col codec: corrupted entry");
      const auto len = lcp + dec;
      SDB_ASSERT(!Dedup() || e == m.first_entry ||
                   std::string_view(out + prev_off, prev_len) <
                     std::string_view(out + off, len),
                 "col codec: dictionary entries out of order");
      values[e] = string_t{out + off, static_cast<uint32_t>(len)};
      prev_off = off;
      prev_len = len;
      off += len;
      enc_off += enc;
    }
    SDB_ENSURE(off == m.raw_len && enc_off == enc_end,
               "col codec: corrupted frame");
  }

  void EnsureGroupOffsets(uint32_t f) {
    if (group_offsets_ready[f]) {
      return;
    }
    const auto first = frames[f].first_entry;
    const auto end = FrameEnd(f);
    uint64_t off = frames[f].comp_off;
    auto* out = group_enc_off.data() + group_base[f];
    for (auto e = first; e < end; ++e) {
      if ((e - first) % kChainRestart == 0) {
        *out++ = static_cast<uint32_t>(off);
      }
      off += length_reader.At(e);
    }
    SDB_ENSURE(
      off == static_cast<uint64_t>(frames[f].comp_off) + frames[f].comp_len,
      "col codec: corrupted front coding");
    group_offsets_ready[f] = 1;
  }

  void DecodeGroup(uint32_t f, uint32_t g) {
    EnsureGroupOffsets(f);
    const auto first = frames[f].first_entry + g * kChainRestart;
    const auto end = std::min<uint32_t>(first + kChainRestart, FrameEnd(f));
    uint64_t capacity = 0;
    for (auto e = first; e < end; ++e) {
      capacity += (e == first ? 0 : lcp_reader.At(e)) +
                  uint64_t{length_reader.At(e)} * kFsstMaxExpansion;
    }
    capacity = std::max<uint64_t>(capacity, string_t::INLINE_LENGTH + 1);
    char* out = duckdb::StringVector::EmptyString(dictionary->data, capacity)
                  .GetDataWriteable();
    const auto* in = reinterpret_cast<const char*>(base + header.off_data);
    uint64_t enc_off = group_enc_off[group_base[f] + g];
    uint64_t off = 0;
    uint64_t prev_off = 0;
    uint64_t prev_len = 0;
    for (auto e = first; e < end; ++e) {
      const uint64_t lcp = e == first ? 0 : lcp_reader.At(e);
      const uint64_t enc = length_reader.At(e);
      SDB_ENSURE(lcp <= prev_len && enc_off + enc <= header.data_size &&
                   off + lcp <= capacity,
                 "col codec: corrupted front coding");
      std::memcpy(out + off, out + prev_off, lcp);
      const auto room = capacity - (off + lcp);
      const auto dec =
        enc == 0 ? 0 : fsst->Decode(in + enc_off, enc, out + off + lcp, room);
      SDB_ENSURE(dec <= room, "col codec: corrupted entry");
      const auto len = lcp + dec;
      values[e] = string_t{out + off, static_cast<uint32_t>(len)};
      prev_off = off;
      prev_len = len;
      off += len;
      enc_off += enc;
    }
    group_decoded[group_base[f] + g] = 1;
  }

  std::string_view DecodeHead(uint32_t f, uint32_t g) {
    EnsureGroupOffsets(f);
    const auto e = frames[f].first_entry + g * kChainRestart;
    if (decoded[f] || group_decoded[group_base[f] + g]) {
      return View(values[e]);
    }
    const auto enc = length_reader.At(e);
    probe.resize(uint64_t{enc} * kFsstMaxExpansion + 1);
    const auto* in = reinterpret_cast<const char*>(base + header.off_data);
    const auto n = enc == 0
                     ? 0
                     : fsst->Decode(in + group_enc_off[group_base[f] + g], enc,
                                    probe.data(), probe.size());
    SDB_ENSURE(n <= probe.size(), "col codec: corrupted entry");
    probe.resize(n);
    return probe;
  }

  template<typename Before>
  uint32_t Bound(Before before) {
    const auto n = header.entry_count;
    if (full) {
      uint32_t lo = FirstEntry();
      uint32_t hi = n;
      while (lo < hi) {
        const auto mid = lo + (hi - lo) / 2;
        if (before(View(values[mid]))) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      return lo;
    }
    if (frames.empty()) {
      return n;
    }
    uint32_t lo = 0;
    uint32_t hi = static_cast<uint32_t>(frames.size());
    while (lo < hi) {
      const auto mid = lo + (hi - lo) / 2;
      if (before(DecodeHead(mid, 0))) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    if (lo == 0) {
      return frames[0].first_entry;
    }
    const uint32_t f = lo - 1;
    uint32_t glo = 1;
    uint32_t ghi = group_base[f + 1] - group_base[f];
    while (glo < ghi) {
      const auto mid = glo + (ghi - glo) / 2;
      if (before(DecodeHead(f, mid))) {
        glo = mid + 1;
      } else {
        ghi = mid;
      }
    }
    const uint32_t g = glo - 1;
    const auto first = frames[f].first_entry + g * kChainRestart;
    const auto end = std::min<uint32_t>(first + kChainRestart, FrameEnd(f));
    if (decoded[f] || group_decoded[group_base[f] + g]) {
      for (auto e = first; e < end; ++e) {
        if (!before(View(values[e]))) {
          return e;
        }
      }
      return end;
    }
    EnsureGroupOffsets(f);
    const auto* in = reinterpret_cast<const char*>(base + header.off_data);
    uint64_t enc_off = group_enc_off[group_base[f] + g];
    probe_prev.clear();
    for (auto e = first; e < end; ++e) {
      const uint64_t lcp = e == first ? 0 : lcp_reader.At(e);
      const uint64_t enc = length_reader.At(e);
      SDB_ENSURE(lcp <= probe_prev.size() && enc_off + enc <= header.data_size,
                 "col codec: corrupted front coding");
      probe.assign(probe_prev, 0, lcp);
      probe.resize(lcp + enc * kFsstMaxExpansion);
      const auto dec = enc == 0
                         ? 0
                         : fsst->Decode(in + enc_off, enc, probe.data() + lcp,
                                        probe.size() - lcp);
      SDB_ENSURE(lcp + dec <= probe.size(), "col codec: corrupted entry");
      probe.resize(lcp + dec);
      if (!before(std::string_view{probe})) {
        return e;
      }
      probe_prev.swap(probe);
      enc_off += enc;
    }
    return end;
  }

  std::pair<uint32_t, uint32_t> EqualRange(std::string_view key) {
    return {Bound([&](std::string_view v) { return v < key; }),
            Bound([&](std::string_view v) { return v <= key; })};
  }

  std::optional<EntryRanges> ResolveFilter(const duckdb::TableFilter& filter) {
    if (filter.filter_type != duckdb::TableFilterType::EXPRESSION_FILTER) {
      return std::nullopt;
    }
    return Resolve(*filter.Cast<duckdb::ExpressionFilter>().expr);
  }

  std::optional<EntryRanges> Resolve(const duckdb::Expression& expr) {
    const uint32_t n = header.entry_count;
    switch (expr.GetExpressionClass()) {
      case duckdb::ExpressionClass::BOUND_OPERATOR: {
        const auto& op = expr.Cast<duckdb::BoundOperatorExpression>();
        const auto& children = op.GetChildren();
        if (children.empty() || !IsColumn(*children[0])) {
          return std::nullopt;
        }
        switch (op.GetExpressionType()) {
          case duckdb::ExpressionType::OPERATOR_IS_NULL:
            if (children.size() != 1) {
              return std::nullopt;
            }
            return EntryRanges{{0, 1}};
          case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL:
            if (children.size() != 1) {
              return std::nullopt;
            }
            return Normalize({{1, n}});
          case duckdb::ExpressionType::COMPARE_IN: {
            if (!Sorted()) {
              return std::nullopt;
            }
            EntryRanges out;
            for (size_t i = 1; i < children.size(); ++i) {
              const auto* value = StringConstant(*children[i]);
              if (!value) {
                return std::nullopt;
              }
              if (!value->IsNull()) {
                out.push_back(EqualRange(duckdb::StringValue::Get(*value)));
              }
            }
            return Normalize(std::move(out));
          }
          default:
            return std::nullopt;
        }
      }
      case duckdb::ExpressionClass::BOUND_FUNCTION: {
        const auto& fn = expr.Cast<duckdb::BoundFunctionExpression>();
        auto comparison = fn.GetExpressionType();
        if (!Sorted() ||
            !duckdb::BoundComparisonExpression::IsComparison(comparison)) {
          return std::nullopt;
        }
        const auto& left = duckdb::BoundComparisonExpression::Left(fn);
        const auto& right = duckdb::BoundComparisonExpression::Right(fn);
        const duckdb::Value* value = nullptr;
        if (IsColumn(left)) {
          value = StringConstant(right);
        } else if (IsColumn(right)) {
          value = StringConstant(left);
          comparison = duckdb::FlipComparisonExpression(comparison);
        }
        if (!value) {
          return std::nullopt;
        }
        switch (comparison) {
          case duckdb::ExpressionType::COMPARE_EQUAL:
          case duckdb::ExpressionType::COMPARE_NOTEQUAL:
          case duckdb::ExpressionType::COMPARE_LESSTHAN:
          case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
          case duckdb::ExpressionType::COMPARE_GREATERTHAN:
          case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            break;
          default:
            return std::nullopt;
        }
        if (value->IsNull()) {
          return EntryRanges{};
        }
        const std::string_view key = duckdb::StringValue::Get(*value);
        const auto below = [&] {
          return Bound([&](std::string_view v) { return v < key; });
        };
        const auto upto = [&] {
          return Bound([&](std::string_view v) { return v <= key; });
        };
        switch (comparison) {
          case duckdb::ExpressionType::COMPARE_EQUAL:
            return Normalize({EqualRange(key)});
          case duckdb::ExpressionType::COMPARE_NOTEQUAL: {
            const auto [lo, hi] = EqualRange(key);
            return Normalize({{1, lo}, {hi, n}});
          }
          case duckdb::ExpressionType::COMPARE_LESSTHAN:
            return Normalize({{1, below()}});
          case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
            return Normalize({{1, upto()}});
          case duckdb::ExpressionType::COMPARE_GREATERTHAN:
            return Normalize({{upto(), n}});
          default:
            return Normalize({{below(), n}});
        }
      }
      case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
        const auto& conjunction =
          expr.Cast<duckdb::BoundConjunctionExpression>();
        const auto kind = conjunction.GetExpressionType();
        if (kind != duckdb::ExpressionType::CONJUNCTION_AND &&
            kind != duckdb::ExpressionType::CONJUNCTION_OR) {
          return std::nullopt;
        }
        std::optional<EntryRanges> acc;
        for (const auto& child : conjunction.GetChildren()) {
          auto ranges = Resolve(*child);
          if (!ranges) {
            return std::nullopt;
          }
          if (!acc) {
            acc = std::move(ranges);
          } else if (kind == duckdb::ExpressionType::CONJUNCTION_AND) {
            acc = Intersect(*acc, *ranges);
          } else {
            acc->insert(acc->end(), ranges->begin(), ranges->end());
            acc = Normalize(std::move(*acc));
          }
        }
        return acc;
      }
      default:
        return std::nullopt;
    }
  }

  const duckdb::SelectionVector& Codes(idx_t start, idx_t count) {
    const auto start_offset = UnpackCodes(start, count);
    if (start_offset != 0) {
      auto* codes = sel->data();
      std::memmove(codes, codes + start_offset, count * sizeof(duckdb::sel_t));
    }
    return *sel;
  }

  const duckdb::SelectionVector& Codes(idx_t start, idx_t span,
                                       const duckdb::SelectionVector& rows,
                                       idx_t row_count) {
    const auto start_offset = UnpackCodes(start, span);
    auto* codes = sel->data();
    for (idx_t i = 0; i < row_count; ++i) {
      codes[i] = codes[start_offset + rows.get_index(i)];
    }
    return *sel;
  }

  void WriteCodes(duckdb::Vector& result, idx_t result_offset,
                  const duckdb::SelectionVector& codes, idx_t count) {
    duckdb::StringVector::AddHeapReference(result, dictionary->data);
    auto writer =
      duckdb::FlatVector::Writer<string_t>(result, count, result_offset);
    for (idx_t i = 0; i < count; ++i) {
      const auto code = codes.get_index(i);
      if (code == 0) {
        writer.WriteNull();
      } else {
        writer.WriteStringRef(values[code]);
      }
    }
  }

  void ScanFlat(duckdb::Vector& result, idx_t result_offset, idx_t start,
                idx_t count) {
    if (!Dedup()) {
      if (count != 0) {
        EnsureEntries(static_cast<uint32_t>(start),
                      static_cast<uint32_t>(start + count - 1));
      }
      duckdb::StringVector::AddHeapReference(result, dictionary->data);
      auto writer =
        duckdb::FlatVector::Writer<string_t>(result, count, result_offset);
      for (idx_t i = 0; i < count; ++i) {
        writer.WriteStringRef(values[start + i]);
      }
      return;
    }
    const auto& codes = Codes(start, count);
    if (!full) {
      if (count * 2 >= STANDARD_VECTOR_SIZE) {
        EnsureDictionary();
      } else {
        EnsureCodes(codes, count);
      }
    }
    WriteCodes(result, result_offset, codes, count);
  }

  void ScanFiltered(duckdb::Vector& result,
                    const duckdb::SelectionVector& codes, idx_t count,
                    const duckdb::SelectionVector& rows, idx_t row_count) {
    const bool sparse = row_count <= kSparseEntries;
    uint32_t f = kNoFrame;
    for (idx_t i = 0; i < row_count; ++i) {
      if (const auto code = codes.get_index(rows.get_index(i)); code != 0) {
        EnsureEntry(static_cast<uint32_t>(code), f, sparse);
      }
    }
    duckdb::StringVector::AddHeapReference(result, dictionary->data);
    auto writer = duckdb::FlatVector::Writer<string_t>(result, count);
    idx_t next = 0;
    for (idx_t row = 0; row < count; ++row) {
      if (next < row_count && rows.get_index(next) == row) {
        ++next;
        if (const auto code = codes.get_index(row); code != 0) {
          writer.WriteStringRef(values[code]);
          continue;
        }
      }
      writer.WriteNull();
    }
  }

  void SelectFlat(duckdb::Vector& result, idx_t start,
                  const duckdb::SelectionVector& rows, idx_t count) {
    SDB_ASSERT(!Dedup());
    EnsureSelected(start, rows, count);
    duckdb::StringVector::AddHeapReference(result, dictionary->data);
    auto writer = duckdb::FlatVector::Writer<string_t>(result, count);
    for (idx_t i = 0; i < count; ++i) {
      writer.WriteStringRef(values[start + rows.get_index(i)]);
    }
  }

  duckdb::BufferHandle handle;
  data_ptr_t base;
  Header header;
  duckdb::LogicalType type;
  PackedReader length_reader;
  PackedReader lcp_reader;
  duckdb::ObjectCache* cache = nullptr;
  std::string cache_key;
  std::atomic<bool>* touched = nullptr;
  bool full = false;
  duckdb::buffer_ptr<duckdb::DictionaryEntry> dictionary;
  string_t* values = nullptr;
  char* heap = nullptr;
  std::vector<FrameMeta> frames;
  std::vector<uint64_t> frame_off;
  std::vector<uint8_t> decoded;
  size_t decoded_count = 0;
  std::vector<uint32_t> group_base;
  std::vector<uint8_t> group_decoded;
  std::vector<uint32_t> group_enc_off;
  std::vector<uint8_t> group_offsets_ready;
  std::string probe;
  std::string probe_prev;
  std::optional<LeafDecompressor<ByteCodec::Lz4>> lz4;
  std::optional<LeafDecompressor<ByteCodec::Zstd>> zstd;
  std::optional<LeafDecompressor<ByteCodec::Zxc>> zxc;
  std::optional<FsstDecoder> fsst;
  duckdb::buffer_ptr<duckdb::SelectionVector> sel;
  idx_t sel_size = 0;
  std::vector<uint32_t> run_values;
  std::vector<uint32_t> run_ends;
  duckdb::unsafe_unique_array<bool> filter_result;
  idx_t filter_match_count = 0;

 private:
  void ReserveCodes(idx_t count) {
    if (!sel || sel_size < count) {
      sel_size = count;
      sel = duckdb::make_buffer<duckdb::SelectionVector>(count);
    }
  }

  idx_t UnpackCodes(idx_t start, idx_t count) {
    SDB_ASSERT(Dedup());
    if (Rle()) {
      ReserveCodes(count);
      auto* codes = sel->data();
      size_t r = static_cast<size_t>(
        std::upper_bound(run_ends.begin(), run_ends.end(), start) -
        run_ends.begin());
      idx_t pos = start;
      idx_t i = 0;
      while (i < count) {
        SDB_ENSURE(r < run_ends.size(), "col codec: corrupted run ends");
        const auto take = std::min<idx_t>(run_ends[r] - pos, count - i);
        std::fill_n(codes + i, take, run_values[r]);
        i += take;
        pos += take;
        ++r;
      }
      return 0;
    }
    const idx_t start_offset = start % kGroup;
    const idx_t decode_count = GroupPadded(count + start_offset);
    ReserveCodes(decode_count);
    auto* src = base + header.off_codes +
                ((start - start_offset) * header.code_width) / 8;
    BitpackingPrimitives::UnPackBuffer<duckdb::sel_t>(
      duckdb::data_ptr_cast(sel->data()), src, decode_count, header.code_width);
    return start_offset;
  }
};

duckdb::unique_ptr<duckdb::SegmentScanState> InitScan(
  const duckdb::QueryContext& /*context*/, duckdb::ColumnSegment& segment) {
  auto& buffer_manager =
    duckdb::BufferManager::GetBufferManager(segment.GetDatabase());
  return duckdb::make_uniq<ScanState>(
    segment, buffer_manager.Pin(segment.GetBlockHandle()));
}

void ScanPartial(duckdb::ColumnSegment& /*segment*/,
                 duckdb::ColumnScanState& state, idx_t scan_count,
                 duckdb::Vector& result, idx_t result_offset) {
  auto& scan = state.scan_state->Cast<ScanState>();
  scan.ScanFlat(result, result_offset, state.GetPositionInSegment(),
                scan_count);
}

void ScanVector(duckdb::ColumnSegment& segment, duckdb::ColumnScanState& state,
                idx_t scan_count, duckdb::Vector& result) {
  auto& scan = state.scan_state->Cast<ScanState>();
  if (!scan.Dedup() || scan_count != STANDARD_VECTOR_SIZE) {
    ScanPartial(segment, state, scan_count, result, 0);
    return;
  }
  scan.EnsureDictionary();
  const auto start = state.GetPositionInSegment();
  result.Dictionary(scan.dictionary, scan.Codes(start, scan_count), scan_count);
}

void Select(duckdb::ColumnSegment& /*segment*/, duckdb::ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result,
            const duckdb::SelectionVector& sel, idx_t sel_count) {
  auto& scan = state.scan_state->Cast<ScanState>();
  const auto start = state.GetPositionInSegment();
  if (!scan.Dedup()) {
    scan.SelectFlat(result, start, sel, sel_count);
    return;
  }
  if (!scan.full && sel_count * 4 < vector_count) {
    const auto& codes = scan.Codes(start, vector_count, sel, sel_count);
    scan.EnsureCodes(codes, sel_count);
    scan.WriteCodes(result, 0, codes, sel_count);
    return;
  }
  scan.EnsureDictionary();
  result.Dictionary(scan.dictionary,
                    scan.Codes(start, vector_count, sel, sel_count), sel_count);
}

void Filter(duckdb::ColumnSegment& segment, duckdb::ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result,
            duckdb::SelectionVector& sel, idx_t& sel_count,
            const duckdb::TableFilter& filter,
            duckdb::TableFilterState& filter_state) {
  auto& scan = state.scan_state->Cast<ScanState>();
  if (!scan.Dedup()) {
    ScanVector(segment, state, vector_count, result);
    duckdb::ColumnSegment::FilterSelection(sel, result, filter_state,
                                           vector_count, sel_count);
    return;
  }
  const auto dict_count = scan.header.entry_count;
  if (!scan.filter_result) {
    scan.filter_result = duckdb::make_unsafe_uniq_array<bool>(dict_count);
    std::memset(scan.filter_result.get(), 0, dict_count);
    if (const auto ranges = scan.ResolveFilter(filter)) {
      idx_t matched = 0;
      for (const auto& [lo, hi] : *ranges) {
        std::memset(scan.filter_result.get() + lo, 1, hi - lo);
        matched += hi - lo;
      }
      scan.filter_match_count = matched;
    } else {
      scan.EnsureDictionary();
      duckdb::SelectionVector dict_sel;
      idx_t filter_count = dict_count;
      duckdb::ColumnSegment::FilterSelection(dict_sel, scan.dictionary->data,
                                             filter_state, dict_count,
                                             filter_count);
      for (idx_t i = 0; i < filter_count; ++i) {
        scan.filter_result[dict_sel.get_index(i)] = true;
      }
      scan.filter_match_count = filter_count;
    }
  }
  if (scan.filter_match_count == 0) {
    sel_count = 0;
    return;
  }
  const auto start = state.GetPositionInSegment();
  const auto& codes = scan.Codes(start, vector_count);
  if (scan.filter_match_count != dict_count) {
    const auto* matches = scan.filter_result.get();
    idx_t kept = 0;
    while (kept < sel_count && matches[codes.get_index(sel.get_index(kept))]) {
      ++kept;
    }
    for (idx_t i = kept + 1; i < sel_count; ++i) {
      const auto row = sel.get_index(i);
      if (matches[codes.get_index(row)]) {
        sel.set_index(kept++, row);
      }
    }
    sel_count = std::min(kept, sel_count);
  }
  if (!scan.full && scan.filter_match_count * 4 >= dict_count) {
    scan.EnsureDictionary();
  }
  if (scan.full) {
    result.Dictionary(scan.dictionary, codes, vector_count);
    return;
  }
  scan.ScanFiltered(result, codes, vector_count, sel, sel_count);
}

struct FetchCache final : duckdb::SegmentScanState {
  const duckdb::ColumnSegment* segment = nullptr;
  uint32_t frame = std::numeric_limits<uint32_t>::max();
  uint32_t first_entry = 0;
  uint32_t hits = 0;
  size_t decoded = 0;
  size_t capacity = 0;
  duckdb::unsafe_unique_array<char> raw;
  std::vector<uint32_t> offsets;
  std::optional<LeafDecompressor<ByteCodec::Lz4>> lz4;
  std::optional<LeafDecompressor<ByteCodec::Zstd>> zstd;
  std::optional<LeafDecompressor<ByteCodec::Zxc>> zxc;
  std::optional<FsstDecoder> fsst;
  std::string prev;
  std::string cur;

  template<typename Decompressor>
  void Decode(Decompressor& d, const FrameMeta& f, const Header& h,
              const_data_ptr_t base, size_t want) {
    SDB_ENSURE(static_cast<uint64_t>(f.comp_off) + f.comp_len <= h.data_size,
               "col codec: corrupted frame table");
    const auto* src =
      reinterpret_cast<const char*>(base + h.off_data) + f.comp_off;
    const size_t got =
      want == f.raw_len
        ? (d.Decompress(src, f.comp_len, raw.get(), f.raw_len) ? f.raw_len : 0)
        : d.DecompressPrefix(src, f.comp_len, raw.get(), want, f.raw_len);
    SDB_ENSURE(got >= want, "col codec: corrupted frame");
    decoded = std::min<size_t>(got, f.raw_len);
  }
};

FetchCache& CacheFor(duckdb::ColumnFetchState& state,
                     const duckdb::ColumnSegment& segment) {
  if (!state.codec_state) {
    state.codec_state = duckdb::make_uniq<FetchCache>();
  }
  auto& cache = state.codec_state->Cast<FetchCache>();
  if (cache.segment != &segment) {
    cache.segment = &segment;
    cache.frame = std::numeric_limits<uint32_t>::max();
    cache.fsst.reset();
  }
  return cache;
}

void FetchRow(duckdb::ColumnSegment& segment, duckdb::ColumnFetchState& state,
              duckdb::row_t row_id, duckdb::Vector& result, idx_t result_idx) {
  auto& handle = state.GetOrInsertHandle(segment);
  auto* base = handle.GetDataMutable() + segment.GetBlockOffset();
  const auto h = Header::Parse(base, segment.SegmentSize());
  const auto row = static_cast<idx_t>(row_id);
  SDB_ENSURE(row < h.row_count, "col codec: row out of range");
  uint32_t entry = static_cast<uint32_t>(row);
  if (h.shape == static_cast<uint8_t>(Shape::Dedup)) {
    if (h.codes_encoding == static_cast<uint8_t>(CodesEncoding::Rle)) {
      PackedReader ends{base + h.off_runs, h.run_width};
      idx_t lo = 0;
      idx_t hi = h.run_count;
      while (lo < hi) {
        const auto mid = lo + (hi - lo) / 2;
        if (ends.At(mid) <= row) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      SDB_ENSURE(lo < h.run_count, "col codec: corrupted run ends");
      entry = PackedReader{base + h.off_codes, h.code_width}.At(lo);
    } else {
      entry = PackedReader{base + h.off_codes, h.code_width}.At(row);
    }
    if (entry == 0) {
      duckdb::FlatVector::ValidityMutable(result).SetInvalid(result_idx);
      return;
    }
  }
  SDB_ENSURE(entry < h.entry_count && h.frame_count != 0,
             "col codec: corrupted row codes");

  const auto* frames = base + h.off_frames;
  uint32_t lo = 0;
  uint32_t hi = h.frame_count;
  while (hi - lo > 1) {
    const auto mid = lo + (hi - lo) / 2;
    if (FrameMeta::Load(frames + mid * kFrameMetaSize).first_entry <= entry) {
      lo = mid;
    } else {
      hi = mid;
    }
  }
  const auto f = FrameMeta::Load(frames + lo * kFrameMetaSize);
  SDB_ENSURE(f.first_entry <= entry, "col codec: corrupted frame table");

  PackedReader lengths{base + h.off_lengths, h.length_width};
  auto* out = duckdb::FlatVector::GetDataMutable<string_t>(result);
  auto& cache = CacheFor(state, segment);
  if (h.codec == static_cast<uint8_t>(ByteCodec::Fsst)) {
    if (!cache.fsst) {
      cache.fsst.emplace();
      SDB_ENSURE(
        h.symtab_size == 0 ||
          cache.fsst->Import(std::string_view{
            reinterpret_cast<const char*>(base + h.off_symtab), h.symtab_size}),
        "col codec: corrupted symbol table");
    }
    PackedReader lcps{base + h.off_lcps, h.lcp_width};
    const auto* in = reinterpret_cast<const char*>(base + h.off_data);
    const uint32_t restart =
      f.first_entry + (entry - f.first_entry) / kChainRestart * kChainRestart;
    uint64_t enc_off = f.comp_off;
    for (uint32_t e = f.first_entry; e < restart; ++e) {
      enc_off += lengths.At(e);
    }
    auto& prev = cache.prev;
    auto& cur = cache.cur;
    prev.clear();
    for (uint32_t e = restart; e <= entry; ++e) {
      const uint64_t lcp = e == restart ? 0 : lcps.At(e);
      const uint64_t enc = lengths.At(e);
      SDB_ENSURE(lcp <= prev.size() && enc_off + enc <= h.data_size,
                 "col codec: corrupted front coding");
      cur.assign(prev, 0, lcp);
      const auto capacity = enc * kFsstMaxExpansion;
      cur.resize(lcp + capacity);
      const auto dec =
        enc == 0
          ? 0
          : cache.fsst->Decode(in + enc_off, enc, cur.data() + lcp, capacity);
      SDB_ENSURE(dec <= capacity, "col codec: corrupted entry");
      cur.resize(lcp + dec);
      prev.swap(cur);
      enc_off += enc;
    }
    out[result_idx] =
      duckdb::StringVector::AddStringOrBlob(result, prev.data(), prev.size());
    return;
  }

  if (cache.frame != lo) {
    const auto end =
      lo + 1 < h.frame_count
        ? FrameMeta::Load(frames + (lo + 1) * kFrameMetaSize).first_entry
        : h.entry_count;
    SDB_ENSURE(end > f.first_entry, "col codec: corrupted frame table");
    cache.offsets.resize(end - f.first_entry + 1);
    uint64_t offset = 0;
    for (uint32_t e = f.first_entry; e < end; ++e) {
      cache.offsets[e - f.first_entry] = static_cast<uint32_t>(offset);
      offset += lengths.At(e);
    }
    SDB_ENSURE(offset == f.raw_len, "col codec: corrupted entry lengths");
    cache.offsets.back() = static_cast<uint32_t>(offset);
    if (cache.capacity < f.raw_len) {
      cache.capacity = std::max<size_t>(f.raw_len, 1);
      cache.raw = duckdb::make_unsafe_uniq_array<char>(cache.capacity);
    }
    cache.frame = lo;
    cache.first_entry = f.first_entry;
    cache.hits = 0;
    cache.decoded = 0;
  }
  ++cache.hits;
  const auto k = entry - cache.first_entry;
  const auto offset = cache.offsets[k];
  const auto len = cache.offsets[k + 1] - offset;
  const size_t want = offset + len;
  if (cache.decoded < want) {
    const size_t target = cache.hits > 1 ? f.raw_len : want;
    switch (static_cast<ByteCodec>(h.codec)) {
      case ByteCodec::Lz4:
        if (!cache.lz4) {
          cache.lz4.emplace();
        }
        cache.Decode(*cache.lz4, f, h, base, target);
        break;
      case ByteCodec::Zstd:
        if (!cache.zstd) {
          cache.zstd.emplace();
        }
        cache.Decode(*cache.zstd, f, h, base, target);
        break;
      case ByteCodec::Zxc:
        if (!cache.zxc) {
          cache.zxc.emplace();
        }
        cache.Decode(*cache.zxc, f, h, base, target);
        break;
      case ByteCodec::Fsst:
        SDB_UNREACHABLE();
    }
  }
  out[result_idx] = duckdb::StringVector::AddStringOrBlob(
    result, cache.raw.get() + offset, len);
}

duckdb::InsertionOrderPreservingMap<std::string> SegmentInfo(
  duckdb::QueryContext /*context*/, duckdb::ColumnSegment& segment) {
  auto& buffer_manager =
    duckdb::BufferManager::GetBufferManager(segment.GetDatabase());
  auto handle = buffer_manager.Pin(segment.GetBlockHandle());
  const auto h = Header::Parse(handle.Ptr() + segment.GetBlockOffset(),
                               segment.SegmentSize());
  duckdb::InsertionOrderPreservingMap<std::string> info;
  info["shape"] =
    h.shape == static_cast<uint8_t>(Shape::Dedup) ? "dedup" : "plain";
  switch (static_cast<ByteCodec>(h.codec)) {
    case ByteCodec::Lz4:
      info["codec"] = "lz4";
      break;
    case ByteCodec::Zstd:
      info["codec"] = "zstd";
      break;
    case ByteCodec::Zxc:
      info["codec"] = "zxc";
      break;
    case ByteCodec::Fsst:
      info["codec"] = "fsst";
      break;
  }
  info["level"] = absl::StrCat(static_cast<uint32_t>(h.level));
  info["codes"] = h.codes_encoding == static_cast<uint8_t>(CodesEncoding::Rle)
                    ? "rle"
                    : "bitpack";
  info["runs"] = absl::StrCat(h.run_count);
  info["entries"] = absl::StrCat(h.entry_count);
  info["frames"] = absl::StrCat(h.frame_count);
  info["raw_bytes"] = absl::StrCat(h.raw_bytes);
  info["data_bytes"] = absl::StrCat(h.data_size);
  return info;
}

duckdb::CompressionFunction MakeScanFunction(
  duckdb::CompressionType type, duckdb::CompressionValidity validity) {
  duckdb::CompressionFunction f{
    type,
    duckdb::PhysicalType::VARCHAR,
    /*init_analyze=*/nullptr,
    /*analyze=*/nullptr,
    /*final_analyze=*/nullptr,
    /*init_compression=*/nullptr,
    /*compress=*/nullptr,
    /*compress_finalize=*/nullptr,
    InitScan,
    ScanVector,
    ScanPartial,
    FetchRow,
    duckdb::UncompressedFunctions::EmptySkip,
  };
  f.select = Select;
  f.filter = Filter;
  f.get_segment_info = SegmentInfo;
  f.validity = validity;
  return f;
}

}  // namespace

const duckdb::CompressionFunction& StringScanFunction(
  duckdb::CompressionType type) {
  static const auto dict_lz4 =
    MakeScanFunction(duckdb::CompressionType::COMPRESSION_DICT_LZ4,
                     duckdb::CompressionValidity::NO_VALIDITY_REQUIRED);
  static const auto dict_zstd =
    MakeScanFunction(duckdb::CompressionType::COMPRESSION_DICT_ZSTD,
                     duckdb::CompressionValidity::NO_VALIDITY_REQUIRED);
  static const auto lz4 =
    MakeScanFunction(duckdb::CompressionType::COMPRESSION_LZ4,
                     duckdb::CompressionValidity::REQUIRES_VALIDITY);
  static const auto dict_fsst =
    MakeScanFunction(duckdb::CompressionType::COMPRESSION_COL_DICT_FSST,
                     duckdb::CompressionValidity::NO_VALIDITY_REQUIRED);
  static const auto fsst =
    MakeScanFunction(duckdb::CompressionType::COMPRESSION_COL_FSST,
                     duckdb::CompressionValidity::REQUIRES_VALIDITY);
  static const auto dict_zxc =
    MakeScanFunction(duckdb::CompressionType::COMPRESSION_DICT_ZXC,
                     duckdb::CompressionValidity::NO_VALIDITY_REQUIRED);
  static const auto zxc =
    MakeScanFunction(duckdb::CompressionType::COMPRESSION_ZXC,
                     duckdb::CompressionValidity::REQUIRES_VALIDITY);
  static const auto zstd =
    MakeScanFunction(duckdb::CompressionType::COMPRESSION_COL_ZSTD,
                     duckdb::CompressionValidity::REQUIRES_VALIDITY);
  switch (type) {
    case duckdb::CompressionType::COMPRESSION_DICT_LZ4:
      return dict_lz4;
    case duckdb::CompressionType::COMPRESSION_DICT_ZSTD:
      return dict_zstd;
    case duckdb::CompressionType::COMPRESSION_LZ4:
      return lz4;
    case duckdb::CompressionType::COMPRESSION_COL_DICT_FSST:
      return dict_fsst;
    case duckdb::CompressionType::COMPRESSION_COL_FSST:
      return fsst;
    case duckdb::CompressionType::COMPRESSION_DICT_ZXC:
      return dict_zxc;
    case duckdb::CompressionType::COMPRESSION_ZXC:
      return zxc;
    case duckdb::CompressionType::COMPRESSION_COL_ZSTD:
      return zstd;
    default:
      SDB_UNREACHABLE();
  }
}

}  // namespace irs::codecs
