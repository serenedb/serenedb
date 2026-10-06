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
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/storage/statistics/list_stats.hpp>
#include <optional>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

class ListColumnReader final : public ColumnReader {
 public:
  ListColumnReader(field_id id, duckdb::LogicalType type,
                   std::vector<ColumnBlockMeta> segments,
                   std::unique_ptr<ColumnReader> validity,
                   std::vector<std::unique_ptr<ColumnReader>> children)
    : ColumnReader{id, std::move(type), std::move(segments),
                   std::move(validity), std::move(children)} {
    auto stats = duckdb::ListStats::CreateEmpty(_type);
    duckdb::ListStats::SetChildStats(
      stats, _children.front()->MergedStatistics().ToUnique());
    FinishStats(std::move(stats));
  }

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const final {
    NewOutputVector(s);
    if (Deduplicated() && count != 0) {
      return ScanCodes<true>(s, result, count, 0);
    }
    return ScanCount(s, result, count, /*result_offset=*/0);
  }

  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const final {
    if (count == 0) {
      return 0;
    }
    if (Deduplicated()) {
      return ScanCodes<false>(s, result, count, result_offset);
    }
    std::optional<duckdb::Vector> big;
    auto& offsets = Scratch(s, count, big);
    const auto scan_count =
      ScanVector(s, offsets, count, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count > 0);
    if (_validity) {
      _validity->ColumnReader::ScanCount(s.child_states[0], result, count,
                                         result_offset);
    }
    const auto* odata = duckdb::FlatVector::GetData<uint64_t>(offsets);
    const uint64_t last_entry = odata[scan_count - 1];
    const uint64_t base = s.st.last_offset;
    const uint64_t child_base = ChildBase(result, result_offset);
    FillEntries(
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result) +
        result_offset,
      odata, scan_count, base, child_base);
    const uint64_t child_scan_count = last_entry - base;
    duckdb::ListVector::Reserve(result, child_base + child_scan_count);
    if (child_scan_count > 0) {
      auto& child = duckdb::ListVector::GetChildMutable(result);
      _children[0]->ScanCount(s.child_states[1], child,
                              static_cast<duckdb::idx_t>(child_scan_count),
                              child_base);
    }
    s.st.last_offset = last_entry;
    duckdb::ListVector::SetListSize(result, child_base + child_scan_count);
    return scan_count;
  }

  void Skip(ScanState& s, duckdb::idx_t count) const final {
    if (_validity) {
      _validity->ColumnReader::Skip(s.child_states[0], count);
    }
    if (Deduplicated()) {
      SkipRows(s, count);
      return;
    }
    if (count > 1) {
      SkipRows(s, count - 1);
    }
    std::optional<duckdb::Vector> big;
    auto& offsets = Scratch(s, 1, big);
    const auto scan_count =
      ScanVector(s, offsets, 1, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count == 1);
    const uint64_t last_entry =
      duckdb::FlatVector::GetData<uint64_t>(offsets)[0];
    const uint64_t child_skip = last_entry - s.st.last_offset;
    s.st.last_offset = last_entry;
    if (child_skip > 0) {
      _children[0]->Skip(s.child_states[1],
                         static_cast<duckdb::idx_t>(child_skip));
    }
  }

  IRS_COLUMN_READER_GATHER_SCATTER
  IRS_COLUMN_READER_GATHER_DENSE

 private:
  static constexpr duckdb::idx_t kPointRows = 16;
  static constexpr uint64_t kReadAheadLists = 2048;
  static constexpr uint64_t kWholeBlockElems = 1 << 19;
  static constexpr uint64_t kWholeBlockLists = 8192;

  bool Deduplicated() const noexcept { return _children.size() == 2; }

  duckdb::Vector& Scratch(ScanState& s, duckdb::idx_t count,
                          std::optional<duckdb::Vector>& big) const {
    if (count > STANDARD_VECTOR_SIZE) {
      return big.emplace(duckdb::LogicalType::UBIGINT, count);
    }
    if (!s.list_offsets) {
      s.list_offsets =
        std::make_unique<VectorScratch>(duckdb::LogicalType::UBIGINT);
    }
    return s.list_offsets->Reset();
  }

  static uint64_t ChildBase(const duckdb::Vector& result,
                            duckdb::idx_t result_offset) {
    return result_offset != 0 ? duckdb::ListVector::GetListSize(result) : 0;
  }

  static void FillEntries(duckdb::list_entry_t* entries, const uint64_t* ends,
                          uint64_t count, uint64_t first,
                          uint64_t child_base) noexcept {
    uint64_t prev = first;
    for (uint64_t k = 0; k < count; ++k) {
      entries[k] =
        duckdb::list_entry_t{child_base + (prev - first), ends[k] - prev};
      prev = ends[k];
    }
  }

  static ListDictionary& Dictionary(ScanState& s) {
    if (!s.list_dict) {
      s.list_dict = std::make_unique<ListDictionary>();
    }
    return *s.list_dict;
  }

  void SeekChild(ScanState& s, size_t child, uint64_t& pos,
                 uint64_t target) const {
    auto& state = s.child_states[child + 1];
    if (target < pos) {
      state = _children[child]->InitScan(s.ctx);
      pos = 0;
    }
    if (target > pos) {
      _children[child]->Skip(state, static_cast<duckdb::idx_t>(target - pos));
    }
  }

  template<typename Accept>
  bool ReadLists(ScanState& s, ListDictionary& d, uint64_t first,
                 uint64_t count, duckdb::Vector& out,
                 duckdb::list_entry_t* entries, uint64_t child_base,
                 Accept&& accept) const {
    const uint64_t first_end = first == 0 ? 0 : first - 1;
    SeekChild(s, 1, d.ends_pos, first_end);
    const uint64_t end_count = first + count - first_end;
    duckdb::Vector ends_vec{duckdb::LogicalType::UBIGINT, end_count};
    _children[1]->ScanCount(s.child_states[2], ends_vec,
                            static_cast<duckdb::idx_t>(end_count), 0);
    d.ends_pos = first + count;
    const auto* ends = duckdb::FlatVector::GetData<uint64_t>(ends_vec);
    const uint64_t first_elem = first == 0 ? 0 : ends[0];
    const uint64_t last_elem = ends[end_count - 1];
    const uint64_t elem_count = last_elem - first_elem;
    if (!accept(elem_count)) {
      return false;
    }
    FillEntries(entries, ends + (first == 0 ? 0 : 1), count, first_elem,
                child_base);
    duckdb::ListVector::Reserve(
      out, static_cast<duckdb::idx_t>(child_base + elem_count));
    if (elem_count > 0) {
      SeekChild(s, 0, d.elems_pos, first_elem);
      _children[0]->ScanCount(s.child_states[1],
                              duckdb::ListVector::GetChildMutable(out),
                              static_cast<duckdb::idx_t>(elem_count),
                              static_cast<duckdb::idx_t>(child_base));
      d.elems_pos = last_elem;
    }
    duckdb::ListVector::SetListSize(
      out, static_cast<duckdb::idx_t>(child_base + elem_count));
    return true;
  }

  static bool CodeRange(const uint64_t* codes,
                        const duckdb::ValidityMask& validity,
                        duckdb::idx_t offset, duckdb::idx_t count, uint64_t& lo,
                        uint64_t& hi) noexcept {
    bool any = false;
    for (duckdb::idx_t i = 0; i < count; ++i) {
      if (!validity.RowIsValid(offset + i)) {
        continue;
      }
      if (!any) {
        lo = hi = codes[i];
        any = true;
        continue;
      }
      lo = std::min(lo, codes[i]);
      hi = std::max(hi, codes[i]);
    }
    return any;
  }

  static bool Consecutive(const uint64_t* codes, duckdb::idx_t count) noexcept {
    for (duckdb::idx_t i = 1; i < count; ++i) {
      if (codes[i] != codes[0] + i) {
        return false;
      }
    }
    return true;
  }

  void ScanRun(ScanState& s, duckdb::Vector& result, uint64_t first,
               duckdb::idx_t count, duckdb::idx_t result_offset) const {
    ReadLists(s, Dictionary(s), first, count, result,
              duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result) +
                result_offset,
              ChildBase(result, result_offset),
              [](uint64_t) noexcept { return true; });
  }

  [[gnu::noinline]] static bool DirectRun(const ScanState& s, uint64_t first,
                                          duckdb::idx_t count) noexcept {
    if (!s.list_dict) {
      return true;
    }
    const auto& d = *s.list_dict;
    if ((first == 0 ? 0 : first - 1) < d.ends_pos) {
      return false;
    }
    return !(d.lists && first >= d.begin && first + count - 1 < d.end);
  }

  [[gnu::noinline]] bool WidenToBlock(ListDictionary& d, uint64_t lo,
                                      uint64_t hi, uint64_t& first,
                                      uint64_t& last) const {
    const auto w = _children[1]->Locate(lo);
    const uint64_t key = w.block + 1;
    if (hi >= w.end || w.end - w.begin > kWholeBlockLists ||
        d.oversized_block == key) {
      return false;
    }
    if (d.missed_block != key) {
      d.missed_block = key;
      return false;
    }
    first = w.begin;
    last = w.end - 1;
    return true;
  }

  ListDictionary& Lists(ScanState& s, uint64_t lo, uint64_t hi,
                        duckdb::idx_t rows) const {
    auto& d = Dictionary(s);
    if (d.lists && lo >= d.begin && hi < d.end) {
      return d;
    }
    const uint64_t distinct = _children[1]->RowCount();
    SDB_ASSERT(hi < distinct);
    uint64_t first = lo;
    uint64_t last_list = hi;
    bool whole_block = false;
    if (rows > kPointRows) {
      last_list =
        std::min(distinct - 1, std::max(hi, lo + kReadAheadLists - 1));
    } else {
      whole_block = WidenToBlock(d, lo, hi, first, last_list);
    }
    const uint64_t list_count = last_list - first + 1;
    duckdb::Vector lists{_type, static_cast<duckdb::idx_t>(list_count + 1)};
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(lists);
    duckdb::FlatVector::ValidityMutable(lists).SetInvalid(0);
    entries[0] = duckdb::list_entry_t{0, 0};
    if (!ReadLists(s, d, first, list_count, lists, entries + 1, 0,
                   [&](uint64_t elems) noexcept {
                     return !whole_block || elems <= kWholeBlockElems;
                   })) {
      d.oversized_block = d.missed_block;
      d.missed_block = 0;
      return Lists(s, lo, hi, rows);
    }
    d.lists.emplace(std::move(lists));
    d.begin = first;
    d.end = last_list + 1;
    return d;
  }

  template<bool kDictionary>
  duckdb::idx_t ScanCodes(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const {
    std::optional<duckdb::Vector> big;
    auto& codes_vec = Scratch(s, count, big);
    const auto scan_count =
      ScanVector(s, codes_vec, count, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count > 0);
    if (_validity) {
      _validity->ColumnReader::ScanCount(s.child_states[0], result, count,
                                         result_offset);
    }
    const auto& validity = duckdb::FlatVector::Validity(result);
    const auto* codes = duckdb::FlatVector::GetData<uint64_t>(codes_vec);
    if (Consecutive(codes, scan_count) && DirectRun(s, codes[0], scan_count)) {
      ScanRun(s, result, codes[0], scan_count, result_offset);
      return scan_count;
    }
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
    const uint64_t child_base = ChildBase(result, result_offset);
    uint64_t lo = 0;
    uint64_t hi = 0;
    if (!CodeRange(codes, validity, result_offset, scan_count, lo, hi)) {
      for (duckdb::idx_t i = 0; i < scan_count; ++i) {
        entries[result_offset + i] = duckdb::list_entry_t{child_base, 0};
      }
      duckdb::ListVector::SetListSize(result,
                                      static_cast<duckdb::idx_t>(child_base));
      return scan_count;
    }
    auto& d = Lists(s, lo, hi, scan_count);
    if constexpr (kDictionary) {
      duckdb::SelectionVector sel{scan_count};
      for (duckdb::idx_t i = 0; i < scan_count; ++i) {
        sel.set_index(i, validity.RowIsValid(i)
                           ? static_cast<duckdb::idx_t>(codes[i] - d.begin + 1)
                           : 0);
      }
      result.Slice(*d.lists, sel, scan_count);
      return scan_count;
    }
    const auto* lists =
      duckdb::FlatVector::GetData<duckdb::list_entry_t>(*d.lists);
    uint64_t total = 0;
    for (duckdb::idx_t i = 0; i < scan_count; ++i) {
      if (validity.RowIsValid(result_offset + i)) {
        total += lists[codes[i] - d.begin + 1].length;
      }
    }
    duckdb::ListVector::Reserve(result,
                                static_cast<duckdb::idx_t>(child_base + total));
    duckdb::SelectionVector picked{static_cast<duckdb::idx_t>(total)};
    uint64_t pos = 0;
    for (duckdb::idx_t i = 0; i < scan_count; ++i) {
      if (!validity.RowIsValid(result_offset + i)) {
        entries[result_offset + i] = duckdb::list_entry_t{child_base + pos, 0};
        continue;
      }
      const auto& list = lists[codes[i] - d.begin + 1];
      entries[result_offset + i] =
        duckdb::list_entry_t{child_base + pos, list.length};
      for (uint64_t k = 0; k < list.length; ++k) {
        picked.set_index(static_cast<duckdb::idx_t>(pos++),
                         static_cast<duckdb::idx_t>(list.offset + k));
      }
    }
    if (total > 0) {
      duckdb::ImmutableStrings::Copy(
        duckdb::ListVector::GetChild(*d.lists),
        duckdb::ListVector::GetChildMutable(result), picked,
        static_cast<duckdb::idx_t>(total), 0,
        static_cast<duckdb::idx_t>(child_base));
    }
    duckdb::ListVector::SetListSize(
      result, static_cast<duckdb::idx_t>(child_base + total));
    return scan_count;
  }
};

}  // namespace irs
