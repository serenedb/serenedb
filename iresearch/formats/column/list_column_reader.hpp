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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <bit>
#include <duckdb/common/type_visitor.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/storage/statistics/list_stats.hpp>
#include <limits>
#include <span>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

class ListColumnReader final : public ColumnReader {
 public:
  ListColumnReader(field_id id, duckdb::LogicalType type,
                   std::vector<ColumnBlockMeta> segments,
                   std::unique_ptr<ColumnReader> validity,
                   std::vector<std::unique_ptr<ColumnReader>> children)
    : ColumnReader{id, std::move(type), std::move(segments),
                   std::move(validity), std::move(children)} {
    SDB_ENSURE(_children.size() == 2, "list column: corrupted list ends");
    _element_strings = duckdb::TypeVisitor::Contains(
      _children.front()->Type(), [](const duckdb::LogicalType& type) {
        return type.InternalType() == duckdb::PhysicalType::VARCHAR;
      });
    auto stats = duckdb::ListStats::CreateEmpty(_type);
    duckdb::ListStats::SetChildStats(
      stats, _children.front()->MergedStatistics().ToUnique());
    FinishStats(std::move(stats));
  }

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const final {
    NewOutputVector(s);
    return ScanCodes<true>(s, result, count, 0);
  }

  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const final {
    return ScanCodes<false>(s, result, count, result_offset);
  }

  IRS_COLUMN_READER_GATHER_SCATTER
  IRS_COLUMN_READER_GATHER_DENSE

 private:
  static constexpr duckdb::idx_t kPointRows = 16;
  static constexpr uint64_t kReadAheadLists = 2048;
  static constexpr uint64_t kWholeBlockElems = 1 << 19;
  static constexpr uint64_t kWholeBlockLists = 8192;
  static constexpr uint64_t kRunGapLists = 256;
  static constexpr uint64_t kAnyElems = std::numeric_limits<uint64_t>::max();

  bool Reusable(const ScanState& s) const noexcept {
    return !_element_strings || s.ctx.use_count() != 0;
  }

  void NewOutputVector(ScanState& s) const final {
    ColumnReader::NewOutputVector(s);
    if (s.list_dict && !Reusable(s)) {
      s.list_dict->lists.reset();
    }
  }

  duckdb::buffer_ptr<duckdb::DictionaryEntry> NewLists(const ScanState& s,
                                                       uint64_t count) const {
    const auto n = static_cast<duckdb::idx_t>(count + 1);
    auto dictionary =
      duckdb::make_buffer<duckdb::DictionaryEntry>(duckdb::Vector{_type, n});
    duckdb::FlatVector::ValidityMutable(dictionary->data).SetInvalid(0);
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(
      dictionary->data)[0] = duckdb::list_entry_t{0, 0};
    return dictionary;
  }

  void SeekChild(ScanState& s, size_t child, uint64_t target) const {
    const auto& reader = *_children[child];
    auto& state = s.child_states[child + 1];
    uint64_t pos = reader.GatherCursor(state);
    if (target < pos) {
      s.retired_states.push_back(std::move(state));
      state = reader.InitScan(s.ctx);
      pos = 0;
    }
    if (target > pos) {
      reader.Skip(state, static_cast<duckdb::idx_t>(target - pos));
    }
  }

  bool ReadLists(ScanState& s, uint64_t first, uint64_t count,
                 duckdb::Vector& out, duckdb::list_entry_t* entries,
                 uint64_t child_base, uint64_t max_elems) const {
    SDB_ENSURE(first + count <= _children[1]->RowCount(),
               "list column: corrupted list codes");
    const bool chained =
      first == 0 || first == _children[1]->GatherCursor(s.child_states[2]);
    const uint64_t from = chained ? first : first - 1;
    SeekChild(s, 1, from);
    const uint64_t end_count = first + count - from;
    auto& ends_vec =
      ResetScratch(s.list_ends, duckdb::LogicalType::UBIGINT, end_count);
    _children[1]->ScanCount(s.child_states[2], ends_vec,
                            static_cast<duckdb::idx_t>(end_count), 0);
    auto* ends = duckdb::FlatVector::GetDataMutable<uint64_t>(ends_vec);
    const uint64_t first_elem =
      first == 0 ? 0 : (chained ? s.st.last_offset : ends[0]);
    const auto* list_ends = ends + (end_count - count);
    const uint64_t last_elem = list_ends[count - 1];
    s.st.last_offset = last_elem;
    SDB_ENSURE(first_elem <= last_elem && last_elem <= _children[0]->RowCount(),
               "list column: corrupted list ends");
    const uint64_t elem_count = last_elem - first_elem;
    if (elem_count > max_elems) {
      return false;
    }
    uint64_t prev = first_elem;
    bool ordered = true;
    for (uint64_t k = 0; k < count; ++k) {
      ordered &= list_ends[k] >= prev;
      entries[k] = duckdb::list_entry_t{child_base + (prev - first_elem),
                                        list_ends[k] - prev};
      prev = list_ends[k];
    }
    SDB_ENSURE(ordered, "list column: corrupted list ends");
    duckdb::ListVector::Reserve(
      out, static_cast<duckdb::idx_t>(child_base + elem_count));
    if (elem_count > 0) {
      SeekChild(s, 0, first_elem);
      _children[0]->ScanCount(s.child_states[1],
                              duckdb::ListVector::GetChildMutable(out),
                              static_cast<duckdb::idx_t>(elem_count),
                              static_cast<duckdb::idx_t>(child_base));
    }
    duckdb::ListVector::SetListSize(
      out, static_cast<duckdb::idx_t>(child_base + elem_count));
    return true;
  }

  IRS_NO_INLINE bool DirectRun(const ScanState& s, uint64_t first,
                               duckdb::idx_t count) const noexcept {
    if (first < _children[1]->GatherCursor(s.child_states[2])) {
      return false;
    }
    if (!s.list_dict) {
      return true;
    }
    const auto& d = *s.list_dict;
    return !(d.lists && first >= d.begin && first + count - 1 < d.end);
  }

  IRS_NO_INLINE bool WidenToBlock(ListDictionary& d, uint64_t lo, uint64_t hi,
                                  uint64_t& first, uint64_t& last) const {
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

  static bool Covers(const ListDictionary& d, std::span<const uint64_t> codes,
                     uint64_t lo, uint64_t hi) noexcept {
    if (!d.lists || lo < d.begin || hi >= d.end) {
      return false;
    }
    if (d.decoded.empty()) {
      return true;
    }
    return absl::c_all_of(
      codes, [begin = d.begin, words = d.decoded.data()](uint64_t code) {
        const uint64_t off = code - begin;
        return CheckBit(words[off / 64], off % 64);
      });
  }

  static uint64_t NextBit(const std::vector<uint64_t>& bits, uint64_t pos,
                          bool set) noexcept {
    const uint64_t limit = bits.size() * 64;
    if (pos >= limit) {
      return limit;
    }
    const uint64_t flip = set ? 0 : ~uint64_t{0};
    uint64_t w = pos / 64;
    uint64_t word = (bits[w] ^ flip) & (~uint64_t{0} << (pos % 64));
    while (word == 0) {
      if (++w == bits.size()) {
        return limit;
      }
      word = bits[w] ^ flip;
    }
    return w * 64 + static_cast<uint64_t>(std::countr_zero(word));
  }

  static void SetBits(std::vector<uint64_t>& bits, uint64_t from,
                      uint64_t to) noexcept {
    for (; from < to && from % 64 != 0; ++from) {
      SetBit(bits[from / 64], from % 64);
    }
    for (; from + 64 <= to; from += 64) {
      bits[from / 64] = ~uint64_t{0};
    }
    for (; from < to; ++from) {
      SetBit(bits[from / 64], from % 64);
    }
  }

  void ReferencedLists(ScanState& s, ListDictionary& d,
                       std::span<const uint64_t> codes, uint64_t lo,
                       uint64_t hi) const {
    auto& bits = d.decoded;
    bits.assign((hi - lo) / 64 + 1, 0);
    auto* words = bits.data();
    for (const auto code : codes) {
      const uint64_t off = code - lo;
      SetBit(words[off / 64], off % 64);
    }
    const uint64_t limit = bits.size() * 64;
    for (uint64_t from = NextBit(bits, 0, true), prev = from; from < limit;) {
      const uint64_t to = NextBit(bits, from, false);
      if (from > prev && from - prev <= kRunGapLists) {
        SetBits(bits, prev, from);
      }
      prev = to;
      from = NextBit(bits, to, true);
    }
    d.ranks.resize(bits.size());
    uint64_t total = 0;
    for (size_t w = 0; w != bits.size(); ++w) {
      d.ranks[w] = static_cast<uint32_t>(total);
      total += static_cast<uint64_t>(std::popcount(bits[w]));
    }
    auto dictionary = NewLists(s, total);
    auto& lists = dictionary->data;
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(lists);
    uint64_t index = 1;
    for (uint64_t from = NextBit(bits, 0, true); from < limit;) {
      const uint64_t to = NextBit(bits, from, false);
      ReadLists(s, lo + from, to - from, lists, entries + index,
                duckdb::ListVector::GetListSize(lists), kAnyElems);
      index += to - from;
      from = NextBit(bits, to, true);
    }
    d.lists = std::move(dictionary);
    d.begin = lo;
    d.end = hi + 1;
  }

  void Lists(ScanState& s, ListDictionary& d, uint64_t lo, uint64_t hi,
             duckdb::idx_t rows) const {
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
    auto dictionary = NewLists(s, list_count);
    auto& lists = dictionary->data;
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(lists);
    if (!ReadLists(s, first, list_count, lists, entries + 1, 0,
                   whole_block ? kWholeBlockElems : kAnyElems)) {
      d.oversized_block = d.missed_block;
      d.missed_block = 0;
      Lists(s, d, lo, hi, rows);
      return;
    }
    d.lists = std::move(dictionary);
    d.decoded.clear();
    d.begin = first;
    d.end = last_list + 1;
  }

  template<bool kDictionary>
  duckdb::idx_t ScanCodes(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const {
    if (count == 0) {
      return 0;
    }
    auto& codes_vec =
      ResetScratch(s.list_codes, duckdb::LogicalType::UBIGINT, count);
    const auto scan_count =
      ScanVector(s, codes_vec, count, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count > 0);
    if (_validity) {
      _validity->ColumnReader::ScanCount(s.child_states[0], result, count,
                                         result_offset);
    }
    const std::span<const uint64_t> codes{
      duckdb::FlatVector::GetData<uint64_t>(codes_vec), scan_count};
    const uint64_t child_base =
      result_offset != 0 ? duckdb::ListVector::GetListSize(result) : 0;
    if (ConsecutiveRunLength(codes, 0) == codes.size() &&
        DirectRun(s, codes[0], scan_count)) {
      ReadLists(
        s, codes[0], scan_count, result,
        duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result) +
          result_offset,
        child_base, kAnyElems);
      return scan_count;
    }
    FromDictionary<kDictionary>(s, result, codes, result_offset, child_base);
    return scan_count;
  }

  template<bool kDictionary>
  IRS_NO_INLINE void FromDictionary(ScanState& s, duckdb::Vector& result,
                                    std::span<const uint64_t> codes,
                                    duckdb::idx_t result_offset,
                                    uint64_t child_base) const {
    const duckdb::idx_t scan_count = codes.size();
    const auto [lo, hi] = std::ranges::minmax(codes);
    SDB_ENSURE(hi < _children[1]->RowCount(),
               "list column: corrupted list codes");
    if (!s.list_dict) {
      s.list_dict = std::make_unique<ListDictionary>();
    }
    auto& d = *s.list_dict;
    if (!Covers(d, codes, lo, hi)) {
      if (Reusable(s) && hi - lo < 2 * kReadAheadLists) {
        Lists(s, d, lo, hi, scan_count);
      } else {
        ReferencedLists(s, d, codes, lo, hi);
      }
    }
    auto& sel = s.list_sel;
    if (sel.Capacity() < scan_count || sel.sel_data().use_count() != 1) {
      sel.Initialize(std::max<duckdb::idx_t>(scan_count, STANDARD_VECTOR_SIZE));
    }
    const auto& validity = duckdb::FlatVector::Validity(result);
    const duckdb::idx_t first = kDictionary ? 0 : result_offset;
    const auto pick = [&](auto index) {
      for (duckdb::idx_t i = 0; i < scan_count; ++i) {
        sel.set_index(i, validity.RowIsValid(first + i)
                           ? static_cast<duckdb::idx_t>(index(codes[i]))
                           : 0);
      }
    };
    if (d.decoded.empty()) {
      pick([begin = d.begin](uint64_t code) { return code - begin + 1; });
    } else {
      pick([begin = d.begin, words = d.decoded.data(),
            ranks = d.ranks.data()](uint64_t code) {
        const uint64_t off = code - begin;
        const auto below = words[off / 64] & ((uint64_t{1} << (off % 64)) - 1);
        return 1 + ranks[off / 64] +
               static_cast<uint64_t>(std::popcount(below));
      });
    }
    if constexpr (kDictionary) {
      result.Dictionary(d.lists, sel, scan_count);
      return;
    }
    const auto* picks = sel.data();
    const auto* lists =
      duckdb::FlatVector::GetData<duckdb::list_entry_t>(d.lists->data);
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result) +
      result_offset;
    uint64_t total = 0;
    for (duckdb::idx_t i = 0; i < scan_count; ++i) {
      total += lists[picks[i]].length;
    }
    duckdb::ListVector::Reserve(result,
                                static_cast<duckdb::idx_t>(child_base + total));
    auto& picked = s.list_picked;
    if (picked.size() < total) {
      picked.resize(total);
    }
    uint64_t pos = 0;
    for (duckdb::idx_t i = 0; i < scan_count; ++i) {
      const auto& list = lists[picks[i]];
      entries[i] = duckdb::list_entry_t{child_base + pos, list.length};
      for (uint64_t k = 0; k < list.length; ++k) {
        picked[pos++] = static_cast<duckdb::sel_t>(list.offset + k);
      }
    }
    if (total != 0) {
      duckdb::ImmutableStrings::Copy(
        duckdb::ListVector::GetChild(d.lists->data),
        duckdb::ListVector::GetChildMutable(result),
        duckdb::SelectionVector{picked.data(),
                                static_cast<duckdb::idx_t>(total)},
        static_cast<duckdb::idx_t>(total), 0,
        static_cast<duckdb::idx_t>(child_base));
    }
    duckdb::ListVector::SetListSize(
      result, static_cast<duckdb::idx_t>(child_base + total));
  }

  bool _element_strings = false;
};

}  // namespace irs
