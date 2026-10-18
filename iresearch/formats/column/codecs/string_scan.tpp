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

#include <algorithm>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/segment/uncompressed.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/dictionary_cache.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_scan.hpp"
#include "iresearch/formats/column/immutable_payloads.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

using duckdb::const_data_ptr_t;
using duckdb::data_ptr_t;
using duckdb::idx_t;
using duckdb::string_t;

template<Shape S, typename Leaf>
struct EntryFrames {
  static constexpr bool kDedup = S == Shape::Dedup;
  static constexpr uint32_t kFirstEntry = kDedup ? 1 : 0;

  EntryFrames(duckdb::ColumnSegment& segment, duckdb::BufferHandle handle_p,
              bool load)
    : handle{std::move(handle_p)},
      base{handle.GetDataMutable() + segment.GetBlockOffset()},
      header{Header::Parse(base, segment.SegmentSize())},
      type{segment.GetType()},
      length_reader{base + header.off_lengths, header.length_width} {
    SDB_ENSURE(header.shape == static_cast<uint8_t>(S) &&
                 header.codec == static_cast<uint8_t>(Leaf::kCodec) &&
                 header.row_count == segment.count &&
                 (kDedup || header.entry_count == header.row_count),
               "col codec: corrupted segment header");
    if (!load) {
      return;
    }
    if constexpr (kDedup) {
      dictionary = duckdb::DictionaryVector::CreateReusableDictionary(
        type, header.entry_count);
    } else {
      dictionary = duckdb::make_buffer<duckdb::DictionaryEntry>(
        duckdb::Vector{type, header.entry_count});
      duckdb::FlatVector::SetSize(dictionary->data, header.entry_count);
    }
    auto& dict_data = dictionary->data;
    values = duckdb::FlatVector::GetDataMutable<string_t>(dict_data);
    const auto heap_bytes =
      std::max<idx_t>(header.raw_bytes, string_t::INLINE_LENGTH + 1);
    heap = duckdb::StringVector::EmptyString(dict_data, heap_bytes)
             .GetDataWriteable();
    duckdb::StringVector::AddAuxiliaryData(
      dict_data, duckdb::make_uniq<ImmutablePayloads>());
    LoadFrames();
    leaf.emplace(segment, *this);
  }

  void LoadFrames() {
    frames.resize(header.frame_count);
    frame_off.resize(header.frame_count + 1);
    decoded.assign(header.frame_count, 0);
    uint64_t raw = 0;
    for (uint32_t i = 0; i < header.frame_count; ++i) {
      const auto f =
        FrameMeta::Load(base + header.off_frames + i * kFrameMetaSize);
      SDB_ENSURE(
        (i == 0 ? f.first_entry == kFirstEntry
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
    SDB_ENSURE(raw == header.raw_bytes &&
                 (header.frame_count != 0 || header.entry_count == kFirstEntry),
               "col codec: corrupted frame table");
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

  bool Complete() const noexcept { return decoded_count == frames.size(); }

  void DecodeAll() {
    for (uint32_t f = 0; f < frames.size(); ++f) {
      if (!decoded[f]) {
        DecodeFrame(f);
      }
    }
  }

  void DecodeFrame(uint32_t f) {
    SDB_ASSERT(!decoded[f]);
    leaf->DecodeFrame(*this, f, heap + frame_off[f]);
    decoded[f] = 1;
    ++decoded_count;
  }

  void EnsureEntry(uint32_t e, uint32_t& f, bool sparse) {
    if (f == kNoFrame || e < frames[f].first_entry || e >= FrameEnd(f)) {
      f = FrameOf(e);
    }
    if (decoded[f]) {
      return;
    }
    if constexpr (Leaf::kSparseGroups) {
      if (sparse) {
        leaf->EnsureEntry(*this, e, f);
        return;
      }
    }
    DecodeFrame(f);
  }

  template<typename Entry>
  void EnsureEach(idx_t count, Entry entry_at) {
    if (Complete()) {
      return;
    }
    const bool sparse = count <= kSparseEntries;
    uint32_t f = kNoFrame;
    for (idx_t i = 0; i < count; ++i) {
      const auto e = static_cast<uint32_t>(entry_at(i));
      if (e >= kFirstEntry) {
        EnsureEntry(e, f, sparse);
      }
    }
  }

  const char* Data() const noexcept {
    return reinterpret_cast<const char*>(base + header.off_data);
  }

  bool Keep(uint32_t e, std::string_view v) noexcept {
    values[e] = string_t{v.data(), static_cast<uint32_t>(v.size())};
    return true;
  }

  uint64_t CacheBytes() const noexcept {
    return header.raw_bytes + (leaf ? leaf->HeapBytes() : 0) +
           static_cast<uint64_t>(header.entry_count) * (sizeof(string_t) + 1);
  }

  void ReferenceHeap(duckdb::Vector& result) const {
    duckdb::StringVector::AddAuxiliaryData(
      result, duckdb::ImmutableStrings::Reference(
                dictionary->data.GetBufferRef()->GetAuxiliaryData()));
  }

  duckdb::BufferHandle handle;
  data_ptr_t base;
  Header header;
  duckdb::LogicalType type;
  PackedReader length_reader;
  duckdb::buffer_ptr<duckdb::DictionaryEntry> dictionary;
  string_t* values = nullptr;
  char* heap = nullptr;
  std::vector<FrameMeta> frames;
  std::vector<uint64_t> frame_off;
  std::vector<uint8_t> decoded;
  size_t decoded_count = 0;
  std::optional<typename Leaf::Scan> leaf;
};

template<typename Leaf>
struct FetchFrames {
  bool Switch(duckdb::ColumnSegment& segment, data_ptr_t base, Shape shape) {
    const auto id = segment.GetBlockHandle()->BlockId();
    if (block == id) {
      return false;
    }
    header = Header::Parse(base, segment.SegmentSize());
    SDB_ENSURE(header.shape == static_cast<uint8_t>(shape) &&
                 header.codec == static_cast<uint8_t>(Leaf::kCodec),
               "col codec: corrupted segment header");
    block = id;
    lengths = PackedReader{base + header.off_lengths, header.length_width};
    leaf.Reset(header, base);
    decoded.reset();
    return true;
  }

  IRS_FORCE_INLINE void Emit(duckdb::ColumnSegment& segment, data_ptr_t base,
                             uint32_t entry, duckdb::Vector& result, idx_t at,
                             bool batched) {
    SDB_ENSURE(header.frame_count != 0, "col codec: corrupted row codes");
    const auto* table = base + header.off_frames;
    uint32_t lo = 0;
    uint32_t hi = header.frame_count;
    while (hi - lo > 1) {
      const auto mid = lo + (hi - lo) / 2;
      if (FrameMeta::Load(table + mid * kFrameMetaSize).first_entry <= entry) {
        lo = mid;
      } else {
        hi = mid;
      }
    }
    const auto f = FrameMeta::Load(table + lo * kFrameMetaSize);
    SDB_ENSURE(f.first_entry <= entry, "col codec: corrupted frame table");
    leaf.Emit(segment, header, base, lengths, lo, f, entry, result, at,
              batched);
  }

  duckdb::block_id_t block = INVALID_BLOCK;
  Header header;
  PackedReader lengths{nullptr, 0};
  ScratchVector<EntryAt> order;
  duckdb::shared_ptr<DecodedDictionary> decoded;
  typename Leaf::Fetch leaf;
};

template<typename Scan>
duckdb::unique_ptr<duckdb::SegmentScanState> InitScan(
  const duckdb::QueryContext&, duckdb::ColumnSegment& segment) {
  auto& buffer_manager =
    duckdb::BufferManager::GetBufferManager(segment.GetDatabase());
  return duckdb::make_uniq<Scan>(segment,
                                 buffer_manager.Pin(segment.GetBlockHandle()));
}

template<typename Scan>
void ScanPartial(duckdb::ColumnSegment&, duckdb::ColumnScanState& state,
                 idx_t scan_count, duckdb::Vector& result,
                 idx_t result_offset) {
  state.scan_state->Cast<Scan>().ScanFlat(
    result, result_offset, state.GetPositionInSegment(), scan_count);
}

template<typename Scan>
void ScanVector(duckdb::ColumnSegment&, duckdb::ColumnScanState& state,
                idx_t scan_count, duckdb::Vector& result) {
  state.scan_state->Cast<Scan>().ScanVector(
    result, state.GetPositionInSegment(), scan_count);
}

template<typename Scan>
void Select(duckdb::ColumnSegment&, duckdb::ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result,
            const duckdb::SelectionVector& sel, idx_t sel_count) {
  state.scan_state->Cast<Scan>().Select(result, state.GetPositionInSegment(),
                                        vector_count, sel, sel_count);
}

template<typename Scan>
void Filter(duckdb::ColumnSegment&, duckdb::ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result,
            duckdb::SelectionVector& sel, idx_t& sel_count,
            const duckdb::TableFilter& filter,
            duckdb::TableFilterState& filter_state) {
  state.scan_state->Cast<Scan>().Filter(result, state.GetPositionInSegment(),
                                        vector_count, sel, sel_count, filter,
                                        filter_state);
}

template<typename Scan>
duckdb::shared_ptr<DecodedDictionary> DecodeWhole(
  duckdb::ColumnSegment& segment) {
  auto& buffer_manager =
    duckdb::BufferManager::GetBufferManager(segment.GetDatabase());
  Scan scan{segment, buffer_manager.Pin(segment.GetBlockHandle())};
  scan.EnsureDictionary();
  return duckdb::make_shared_ptr<DecodedDictionary>(scan.entries.dictionary,
                                                    scan.entries.CacheBytes());
}

template<typename Scan>
void FetchRows(duckdb::ColumnSegment& segment, duckdb::ColumnFetchState& state,
               const idx_t* rows, idx_t count, duckdb::Vector& result,
               const duckdb::SelectionVector& result_sel, idx_t result_offset) {
  using Fetch = typename Scan::Fetch;
  auto& handle = state.GetOrInsertHandle(segment);
  auto* base = handle.GetDataMutable() + segment.GetBlockOffset();
  auto& fetch = state.GetOrCreateSegmentState<Fetch>(
    segment, [] { return duckdb::make_uniq<Fetch>(); });
  auto& frames = fetch.frames;
  if (frames.Switch(segment, base, Scan::kShape)) {
    fetch.Reset(segment, base);
  }
  if (count == 1) {
    const auto at = result_offset + result_sel.get_index(0);
    const auto entry = fetch.ReadOne(base, rows[0]);
    if (!entry) {
      duckdb::FlatVector::ValidityMutable(result).SetInvalid(at);
      return;
    }
    if (!frames.decoded && fetch.Whole()) {
      frames.decoded = DecodeWhole<Scan>(segment);
    }
    if (frames.decoded) {
      const EntryAt one{.entry = *entry, .at = at};
      EmitDecoded(*frames.decoded, std::span<const EntryAt>{&one, 1}, result);
      return;
    }
    frames.Emit(segment, base, *entry, result, at, false);
    if (fetch.Whole()) {
      frames.decoded = DecodeWhole<Scan>(segment);
    }
    return;
  }
  fetch.Read(base, rows, count, result, result_sel, result_offset);
  auto& order = frames.order;
  if (!frames.decoded && fetch.Whole()) {
    frames.decoded = DecodeWhole<Scan>(segment);
  }
  if (frames.decoded) {
    EmitDecoded(*frames.decoded, order, result);
    return;
  }
  const bool batched = order.size() > 1;
  if (batched && fetch.PlanBatch(base)) {
    frames.decoded = DecodeWhole<Scan>(segment);
    EmitDecoded(*frames.decoded, order, result);
    return;
  }
  for (size_t i = 0; i < order.size(); ++i) {
    frames.Emit(segment, base, order[i].entry, result, order[i].at, batched);
    if (fetch.Whole()) {
      frames.decoded = DecodeWhole<Scan>(segment);
      EmitDecoded(*frames.decoded,
                  std::span<const EntryAt>{order}.subspan(i + 1), result);
      return;
    }
  }
}

template<typename Scan>
duckdb::CompressionFunction StringFunction(duckdb::CompressionType type) {
  duckdb::CompressionFunction f{
    type,
    duckdb::PhysicalType::VARCHAR,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    InitScan<Scan>,
    ScanVector<Scan>,
    ScanPartial<Scan>,
    FetchRows<Scan>,
    duckdb::UncompressedFunctions::EmptySkip,
  };
  f.select = Select<Scan>;
  f.filter = Filter<Scan>;
  f.get_segment_info = SegmentInfo;
  f.validity = Scan::kValidity;
  return f;
}

}  // namespace irs::codecs::text
