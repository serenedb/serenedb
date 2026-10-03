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
      return ScanDictionary(s, result, count);
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
      return ScanFlat(s, result, count, result_offset);
    }
    // Nested list children can request more than a vector's worth of rows;
    // everything else reuses the state-owned scratch.
    std::optional<duckdb::Vector> big;
    if (count > STANDARD_VECTOR_SIZE) {
      big.emplace(duckdb::LogicalType::UBIGINT, count);
    } else if (!s.list_offsets) {
      s.list_offsets =
        std::make_unique<VectorScratch>(duckdb::LogicalType::UBIGINT);
    }
    duckdb::Vector& offsets = big ? *big : s.list_offsets->Reset();
    const auto scan_count =
      ScanVector(s, offsets, count, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count > 0);
    if (_validity) {
      _validity->ColumnReader::ScanCount(s.child_states[0], result, count,
                                         result_offset);
    }
    const auto* odata = duckdb::FlatVector::GetData<uint64_t>(offsets);
    const uint64_t last_entry = odata[scan_count - 1];
    auto* list_entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
    const uint64_t base = s.st.last_offset;
    const uint64_t child_base =
      result_offset != 0 ? duckdb::ListVector::GetListSize(result) : 0;
    list_entries[result_offset] =
      duckdb::list_entry_t{child_base, odata[0] - base};
    for (duckdb::idx_t i = 1; i < scan_count; ++i) {
      list_entries[result_offset + i] = duckdb::list_entry_t{
        child_base + (odata[i - 1] - base), odata[i] - odata[i - 1]};
    }
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
      if (count > 0) {
        SkipRows(s, count);
      }
      return;
    }
    if (count > 1) {
      SkipRows(s, count - 1);
    }
    if (!s.list_offsets) {
      s.list_offsets =
        std::make_unique<VectorScratch>(duckdb::LogicalType::UBIGINT);
    }
    duckdb::Vector& offsets = s.list_offsets->Reset();
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

  duckdb::Vector& Codes(ScanState& s, duckdb::idx_t count,
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
    if (!s.list_dict) {
      s.list_dict = std::make_unique<ListDictionary>();
    }
    auto& d = *s.list_dict;
    const auto& ends_reader = *_children[1];
    const auto& elems_reader = *_children[0];
    const uint64_t first_end = first == 0 ? 0 : first - 1;
    auto& ends_state = s.child_states[2];
    if (first_end < d.ends_pos) {
      ends_state = ends_reader.InitScan(s.ctx);
      d.ends_pos = 0;
    }
    if (first_end > d.ends_pos) {
      ends_reader.Skip(ends_state, first_end - d.ends_pos);
    }
    const uint64_t end_count = first + count - first_end;
    duckdb::Vector ends_vec{duckdb::LogicalType::UBIGINT, end_count};
    ends_reader.ScanCount(ends_state, ends_vec,
                          static_cast<duckdb::idx_t>(end_count), 0);
    d.ends_pos = first + count;
    const auto* ends = duckdb::FlatVector::GetData<uint64_t>(ends_vec);
    const uint64_t shift = first == 0 ? 0 : 1;
    const uint64_t first_elem = first == 0 ? 0 : ends[0];
    const uint64_t last_elem = ends[end_count - 1];
    const uint64_t child_base =
      result_offset != 0 ? duckdb::ListVector::GetListSize(result) : 0;
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
    uint64_t prev = first_elem;
    for (duckdb::idx_t k = 0; k < count; ++k) {
      const uint64_t end = ends[shift + k];
      entries[result_offset + k] =
        duckdb::list_entry_t{child_base + (prev - first_elem), end - prev};
      prev = end;
    }
    const uint64_t elem_count = last_elem - first_elem;
    duckdb::ListVector::Reserve(
      result, static_cast<duckdb::idx_t>(child_base + elem_count));
    if (elem_count > 0) {
      auto& elems_state = s.child_states[1];
      if (first_elem < d.elems_pos) {
        elems_state = elems_reader.InitScan(s.ctx);
        d.elems_pos = 0;
      }
      if (first_elem > d.elems_pos) {
        elems_reader.Skip(elems_state,
                          static_cast<duckdb::idx_t>(first_elem - d.elems_pos));
      }
      elems_reader.ScanCount(elems_state,
                             duckdb::ListVector::GetChildMutable(result),
                             static_cast<duckdb::idx_t>(elem_count),
                             static_cast<duckdb::idx_t>(child_base));
      d.elems_pos = last_elem;
    }
    duckdb::ListVector::SetListSize(
      result, static_cast<duckdb::idx_t>(child_base + elem_count));
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
    if (!s.list_dict) {
      s.list_dict = std::make_unique<ListDictionary>();
    }
    auto& d = *s.list_dict;
    if (d.lists && lo >= d.begin && hi < d.end) {
      return d;
    }
    const auto& ends_reader = *_children[1];
    const auto& elems_reader = *_children[0];
    const uint64_t distinct = ends_reader.RowCount();
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
    const uint64_t first_end = first == 0 ? 0 : first - 1;
    auto& ends_state = s.child_states[2];
    if (first_end < d.ends_pos) {
      ends_state = ends_reader.InitScan(s.ctx);
      d.ends_pos = 0;
    }
    if (first_end > d.ends_pos) {
      ends_reader.Skip(ends_state, first_end - d.ends_pos);
    }
    const uint64_t end_count = last_list - first_end + 1;
    duckdb::Vector ends_vec{duckdb::LogicalType::UBIGINT, end_count};
    ends_reader.ScanCount(ends_state, ends_vec,
                          static_cast<duckdb::idx_t>(end_count), 0);
    d.ends_pos = last_list + 1;
    const auto* ends = duckdb::FlatVector::GetData<uint64_t>(ends_vec);
    const uint64_t shift = first == 0 ? 0 : 1;
    const uint64_t first_elem = first == 0 ? 0 : ends[0];
    const uint64_t last_elem = ends[end_count - 1];
    const uint64_t list_count = last_list - first + 1;
    if (whole_block && last_elem - first_elem > kWholeBlockElems) {
      d.oversized_block = d.missed_block;
      d.missed_block = 0;
      return Lists(s, lo, hi, rows);
    }

    duckdb::Vector lists{_type, static_cast<duckdb::idx_t>(list_count + 1)};
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(lists);
    duckdb::FlatVector::ValidityMutable(lists).SetInvalid(0);
    entries[0] = duckdb::list_entry_t{0, 0};
    uint64_t prev = first_elem;
    for (uint64_t k = 0; k < list_count; ++k) {
      const uint64_t end = ends[shift + k];
      entries[k + 1] = duckdb::list_entry_t{prev - first_elem, end - prev};
      prev = end;
    }
    const uint64_t elem_count = last_elem - first_elem;
    duckdb::ListVector::Reserve(lists, static_cast<duckdb::idx_t>(elem_count));
    if (elem_count > 0) {
      auto& elems_state = s.child_states[1];
      if (first_elem < d.elems_pos) {
        elems_state = elems_reader.InitScan(s.ctx);
        d.elems_pos = 0;
      }
      if (first_elem > d.elems_pos) {
        elems_reader.Skip(elems_state,
                          static_cast<duckdb::idx_t>(first_elem - d.elems_pos));
      }
      elems_reader.ScanCount(elems_state,
                             duckdb::ListVector::GetChildMutable(lists),
                             static_cast<duckdb::idx_t>(elem_count), 0);
      d.elems_pos = last_elem;
    }
    duckdb::ListVector::SetListSize(lists,
                                    static_cast<duckdb::idx_t>(elem_count));
    d.lists.emplace(std::move(lists));
    d.begin = first;
    d.end = last_list + 1;
    return d;
  }

  duckdb::idx_t ScanDictionary(ScanState& s, duckdb::Vector& result,
                               duckdb::idx_t count) const {
    std::optional<duckdb::Vector> big;
    auto& codes_vec = Codes(s, count, big);
    const auto scan_count =
      ScanVector(s, codes_vec, count, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count > 0);
    if (_validity) {
      _validity->ColumnReader::ScanCount(s.child_states[0], result, count, 0);
    }
    const auto& validity = duckdb::FlatVector::Validity(result);
    const auto* codes = duckdb::FlatVector::GetData<uint64_t>(codes_vec);
    if (Consecutive(codes, scan_count) && DirectRun(s, codes[0], scan_count)) {
      ScanRun(s, result, codes[0], scan_count, 0);
      return scan_count;
    }
    uint64_t lo = 0;
    uint64_t hi = 0;
    if (!CodeRange(codes, validity, 0, scan_count, lo, hi)) {
      auto* entries =
        duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
      for (duckdb::idx_t i = 0; i < scan_count; ++i) {
        entries[i] = duckdb::list_entry_t{0, 0};
      }
      duckdb::ListVector::SetListSize(result, 0);
      return scan_count;
    }
    auto& d = Lists(s, lo, hi, scan_count);
    duckdb::SelectionVector sel{scan_count};
    for (duckdb::idx_t i = 0; i < scan_count; ++i) {
      sel.set_index(i, validity.RowIsValid(i)
                         ? static_cast<duckdb::idx_t>(codes[i] - d.begin + 1)
                         : 0);
    }
    result.Slice(*d.lists, sel, scan_count);
    return scan_count;
  }

  duckdb::idx_t ScanFlat(ScanState& s, duckdb::Vector& result,
                         duckdb::idx_t count,
                         duckdb::idx_t result_offset) const {
    std::optional<duckdb::Vector> big;
    auto& codes_vec = Codes(s, count, big);
    const auto scan_count =
      ScanVector(s, codes_vec, count, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count > 0);
    if (_validity) {
      _validity->ColumnReader::ScanCount(s.child_states[0], result, count,
                                         result_offset);
    }
    const auto& validity = duckdb::FlatVector::Validity(result);
    const auto* codes = duckdb::FlatVector::GetData<uint64_t>(codes_vec);
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
    if (Consecutive(codes, scan_count) && DirectRun(s, codes[0], scan_count)) {
      ScanRun(s, result, codes[0], scan_count, result_offset);
      return scan_count;
    }
    const uint64_t child_base =
      result_offset != 0 ? duckdb::ListVector::GetListSize(result) : 0;
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
