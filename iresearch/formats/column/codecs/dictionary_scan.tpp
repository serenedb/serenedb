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
#include <atomic>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/dictionary_cache.hpp"
#include "iresearch/formats/column/codecs/numeric_decoder.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_scan.hpp"
#include "iresearch/formats/column/codecs/string_scan.tpp"
#include "iresearch/formats/column/immutable_payloads.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

template<typename Leaf>
struct DictionaryFetch final : duckdb::SegmentScanState {
  void Reset(duckdb::ColumnSegment& segment, data_ptr_t base) {
    const auto& h = frames.header;
    code_frames.reset();
    codes = PackedReader{base + h.off_codes, h.code_width};
    ends = PackedReader{base + h.off_runs, h.run_width};
    run = 0;
    last_group = kNoGroup;
    frames.decoded = CachedDictionary(segment);
  }

  void Read(data_ptr_t base, const idx_t* rows, idx_t count,
            duckdb::Vector& result, const duckdb::SelectionVector& sel,
            idx_t offset) {
    const auto& h = frames.header;
    auto& order = frames.order;
    order.resize(count);
    auto* out = order.data();
    auto& validity = duckdb::FlatVector::ValidityMutable(result);
    idx_t kept = 0;
    const auto keep = [&](idx_t i, uint32_t code) {
      const auto at = offset + sel.get_index(i);
      if (code == 0) {
        validity.SetInvalid(at);
        return;
      }
      SDB_ENSURE(code < h.entry_count, "col codec: corrupted row codes");
      out[kept++] = {.entry = code, .at = at};
    };
    switch (static_cast<CodesEncoding>(h.codes_encoding)) {
      case CodesEncoding::Bitpack:
        for (idx_t i = 0; i < count; ++i) {
          if (i + kFetchAhead < count && rows[i + kFetchAhead] < h.row_count) {
            __builtin_prefetch(base + h.off_codes +
                               rows[i + kFetchAhead] * h.code_width / 8);
          }
          SDB_ENSURE(rows[i] < h.row_count, "col codec: row out of range");
          keep(i, PackedCode(rows[i], i + 1 < count ? rows[i + 1] / kGroup
                                                    : last_group));
        }
        break;
      case CodesEncoding::Rle:
        for (idx_t i = 0; i < count; ++i) {
          SDB_ENSURE(rows[i] < h.row_count, "col codec: row out of range");
          keep(i, RunCode(rows[i]));
        }
        break;
      case CodesEncoding::Numeric: {
        auto& numeric = Numeric(base);
        for (idx_t i = 0; i < count; ++i) {
          SDB_ENSURE(rows[i] < h.row_count, "col codec: row out of range");
          keep(i, NumericCode(numeric, rows[i], i == 0 ? kNoRow : rows[i - 1],
                              i + 1 == count ? kNoRow : rows[i + 1]));
        }
        break;
      }
    }
    order.resize(kept);
  }

  IRS_FORCE_INLINE std::optional<uint32_t> ReadOne(data_ptr_t base, idx_t row) {
    const auto& h = frames.header;
    SDB_ENSURE(row < h.row_count, "col codec: row out of range");
    uint32_t code = 0;
    switch (static_cast<CodesEncoding>(h.codes_encoding)) {
      case CodesEncoding::Bitpack:
        code = PackedCode(row, last_group);
        break;
      case CodesEncoding::Rle:
        code = RunCode(row);
        break;
      case CodesEncoding::Numeric:
        code = NumericCode(Numeric(base), row, kNoRow, kNoRow);
        break;
    }
    if (code == 0) {
      return std::nullopt;
    }
    SDB_ENSURE(code < h.entry_count, "col codec: corrupted row codes");
    return code;
  }

  IRS_FORCE_INLINE uint32_t PackedCode(idx_t row, idx_t neighbour) {
    const auto group = row / kGroup;
    const bool shared = group == codes.loaded || group == neighbour;
    last_group = group;
    return shared ? codes.At(row) : codes.Single(row);
  }

  IRS_FORCE_INLINE uint32_t RunCode(idx_t row) {
    const auto& h = frames.header;
    idx_t lo = run;
    if (lo < h.run_count && ends.At(lo) <= row) {
      ++lo;
    }
    if (lo >= h.run_count || ends.At(lo) <= row ||
        (lo != 0 && ends.At(lo - 1) > row)) {
      lo = 0;
      idx_t hi = h.run_count;
      while (lo < hi) {
        const auto mid = lo + (hi - lo) / 2;
        if (ends.Single(mid) <= row) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
    }
    SDB_ENSURE(lo < h.run_count, "col codec: corrupted run ends");
    run = lo;
    return codes.At(lo);
  }

  FrameDecoder<uint32_t>& Numeric(data_ptr_t base) {
    if (!code_frames) {
      code_frames.emplace(base + frames.header.off_codes,
                          CodesHeader(base, frames.header));
    }
    return *code_frames;
  }

  IRS_FORCE_INLINE static uint32_t NumericCode(FrameDecoder<uint32_t>& numeric,
                                               idx_t row, uint64_t previous,
                                               uint64_t next) {
    if (numeric.ReadsAlone() && !numeric.Holds(row) &&
        !numeric.Near(row, previous) && !numeric.Near(row, next)) {
      return numeric.ReadAlone(row);
    }
    numeric.Seek(row);
    return numeric.At(row);
  }

  bool PlanBatch(const_data_ptr_t base) {
    auto& order = frames.order;
    absl::c_sort(order, [](const EntryAt& l, const EntryAt& r) {
      return l.entry > r.entry;
    });
    return 2 * (frames.leaf.DecodedBytes() +
                TouchedBytes(frames.header, base, order)) >=
           frames.header.raw_bytes;
  }

  bool Whole() const noexcept {
    return frames.leaf.Walked() >= frames.header.entry_count ||
           frames.leaf.DecodedBytes() >= frames.header.raw_bytes;
  }

  static constexpr idx_t kNoGroup = std::numeric_limits<idx_t>::max();

  FetchFrames<Leaf> frames;
  std::optional<FrameDecoder<uint32_t>> code_frames;
  PackedReader codes{nullptr, 0};
  PackedReader ends{nullptr, 0};
  idx_t run = 0;
  idx_t last_group = kNoGroup;
};

template<typename L>
struct DictionaryScan final : duckdb::SegmentScanState {
  using Leaf = L;
  using Fetch = DictionaryFetch<L>;
  static constexpr Shape kShape = Shape::Dedup;
  static constexpr auto kValidity =
    duckdb::CompressionValidity::NO_VALIDITY_REQUIRED;

  DictionaryScan(duckdb::ColumnSegment& segment, duckdb::BufferHandle handle)
    : DictionaryScan{segment, std::move(handle), CachedDictionary(segment)} {}

  DictionaryScan(duckdb::ColumnSegment& segment, duckdb::BufferHandle handle,
                 const duckdb::shared_ptr<DecodedDictionary>& cached)
    : entries{segment, std::move(handle), !cached} {
    if (const auto& slot = CacheSlotOf(segment); !slot.key.empty()) {
      cache = &segment.GetDatabase().GetObjectCache();
      cache_key = slot.key;
      touched = slot.touched;
    }
    if (cached) {
      AdoptDictionary(*cached);
    } else {
      entries.values[0] = string_t{};
      duckdb::FlatVector::ValidityMutable(entries.dictionary->data)
        .SetInvalid(0);
      full = entries.frames.empty();
    }
    LoadCodes();
  }

  void AdoptDictionary(const DecodedDictionary& cached) {
    const auto& owner = cached.Dictionary();
    const auto count = entries.header.entry_count;
    duckdb::Vector view{entries.type,
                        duckdb::FlatVector::GetDataMutable(owner->data), count};
    auto& validity = duckdb::FlatVector::ValidityMutable(view);
    validity.Initialize(count);
    validity.SetInvalid(0);
    cold_entries = Leaf::kSorted;
    duckdb::StringVector::AddAuxiliaryData(
      view, duckdb::make_uniq<duckdb::VectorBufferHolder>(
              owner->data.GetBufferRef()));
    duckdb::StringVector::AddAuxiliaryData(
      view, duckdb::make_uniq<ImmutablePayloads>());
    entries.dictionary =
      duckdb::make_buffer<duckdb::DictionaryEntry>(std::move(view));
    entries.dictionary->id = owner->id;
    entries.values =
      duckdb::FlatVector::GetDataMutable<string_t>(entries.dictionary->data);
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
    entries.DecodeAll();
    Admit();
  }

  void Admit() {
    full = true;
    if (!cache || !touched->exchange(true, std::memory_order_relaxed)) {
      return;
    }
    const auto bytes = entries.CacheBytes();
    if (cache->GetCurrentMemory() + bytes <= cache->GetMaxMemory()) {
      cache->Put(cache_key, duckdb::make_shared_ptr<DecodedDictionary>(
                              entries.dictionary, bytes));
    }
  }

  void LoadCodes() {
    const auto& h = entries.header;
    auto* base = entries.base;
    if (h.codes_encoding == static_cast<uint8_t>(CodesEncoding::Numeric)) {
      code_frames.emplace(base + h.off_codes, CodesHeader(base, h));
      return;
    }
    if (!Rle()) {
      return;
    }
    run_values = Unpack(base + h.off_codes, h.run_count, h.code_width);
    run_ends = Unpack(base + h.off_runs, h.run_count, h.run_width);
    SDB_ENSURE(!run_ends.empty() && run_ends.back() == h.row_count,
               "col codec: corrupted run ends");
    SDB_ENSURE(
      absl::c_all_of(run_values,
                     [&](uint32_t code) { return code < h.entry_count; }),
      "col codec: corrupted row codes");
  }

  bool Rle() const noexcept {
    return entries.header.codes_encoding ==
           static_cast<uint8_t>(CodesEncoding::Rle);
  }

  template<typename Entry>
  void EnsureEach(idx_t count, Entry entry_at) {
    if (full) {
      return;
    }
    entries.EnsureEach(count, entry_at);
    if (entries.Complete()) {
      Admit();
    }
  }

  void EnsureCodes(const duckdb::SelectionVector& codes, idx_t count) {
    EnsureEach(count, [&](idx_t i) { return codes.get_index(i); });
  }

  template<typename Before>
  uint32_t Bound(Before before) {
    if (!full) {
      return entries.leaf->Bound(entries, before);
    }
    uint32_t lo = 1;
    uint32_t hi = entries.header.entry_count;
    while (lo < hi) {
      const auto mid = lo + (hi - lo) / 2;
      if (before(View(entries.values[mid]))) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  uint32_t Lower(std::string_view key) {
    return Bound([&](std::string_view v) { return v < key; });
  }

  uint32_t Upper(std::string_view key) {
    return Bound([&](std::string_view v) { return v <= key; });
  }

  std::pair<uint32_t, uint32_t> EqualRange(std::string_view key) {
    return {Lower(key), Upper(key)};
  }

  std::optional<EntryRanges> ResolveFilter(const duckdb::TableFilter& filter) {
    if (filter.filter_type != duckdb::TableFilterType::EXPRESSION_FILTER) {
      return std::nullopt;
    }
    return Resolve(*filter.Cast<duckdb::ExpressionFilter>().expr);
  }

  std::optional<EntryRanges> Resolve(const duckdb::Expression& expr) {
    const uint32_t n = entries.header.entry_count;
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
          case duckdb::ExpressionType::COMPARE_IN:
            if constexpr (Leaf::kSorted) {
              return ResolveIn(children);
            } else {
              return std::nullopt;
            }
          default:
            return std::nullopt;
        }
      }
      case duckdb::ExpressionClass::BOUND_FUNCTION:
        if constexpr (Leaf::kSorted) {
          return ResolveComparison(
            expr.Cast<duckdb::BoundFunctionExpression>());
        } else {
          return std::nullopt;
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

  std::optional<EntryRanges> ResolveIn(const auto& children) {
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

  std::optional<EntryRanges> ResolveComparison(
    const duckdb::BoundFunctionExpression& fn) {
    const uint32_t n = entries.header.entry_count;
    auto comparison = fn.GetExpressionType();
    if (!duckdb::BoundComparisonExpression::IsComparison(comparison)) {
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
    switch (comparison) {
      case duckdb::ExpressionType::COMPARE_EQUAL:
        return Normalize({EqualRange(key)});
      case duckdb::ExpressionType::COMPARE_NOTEQUAL: {
        const auto [lo, hi] = EqualRange(key);
        return Normalize({{1, lo}, {hi, n}});
      }
      case duckdb::ExpressionType::COMPARE_LESSTHAN:
        return Normalize({{1, Lower(key)}});
      case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
        return Normalize({{1, Upper(key)}});
      case duckdb::ExpressionType::COMPARE_GREATERTHAN:
        return Normalize({{Upper(key), n}});
      default:
        return Normalize({{Lower(key), n}});
    }
  }

  const duckdb::SelectionVector& Codes(idx_t start, idx_t count) {
    const auto start_offset = UnpackCodes(start, count);
    if (start_offset != 0) {
      auto* codes = code_sel->data();
      std::memmove(codes, codes + start_offset, count * sizeof(duckdb::sel_t));
    }
    return *code_sel;
  }

  const duckdb::SelectionVector& Codes(idx_t start, idx_t span,
                                       const duckdb::SelectionVector& rows,
                                       idx_t row_count) {
    const auto start_offset = UnpackCodes(start, span);
    auto* codes = code_sel->data();
    for (idx_t i = 0; i < row_count; ++i) {
      codes[i] = codes[start_offset + rows.get_index(i)];
    }
    return *code_sel;
  }

  void WarmEntries() noexcept {
    cold_entries = false;
    constexpr uint32_t kPerLine = 64 / sizeof(string_t);
    uint64_t bytes = 0;
    for (uint32_t e = 0; e < entries.header.entry_count; e += kPerLine) {
      bytes += entries.values[e].GetSize();
    }
    warmed_bytes = bytes;
  }

  void WriteCodes(duckdb::Vector& result, idx_t result_offset,
                  const duckdb::SelectionVector& codes, idx_t count) {
    if (cold_entries) {
      WarmEntries();
    }
    entries.ReferenceHeap(result);
    auto writer =
      duckdb::FlatVector::Writer<string_t>(result, count, result_offset);
    for (idx_t i = 0; i < count; ++i) {
      const auto code = codes.get_index(i);
      if (code == 0) {
        writer.WriteNull();
      } else {
        writer.WriteStringRef(entries.values[code]);
      }
    }
  }

  void ScanFlat(duckdb::Vector& result, idx_t result_offset, idx_t start,
                idx_t count) {
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

  void ScanVector(duckdb::Vector& result, idx_t start, idx_t count) {
    if (count != STANDARD_VECTOR_SIZE) {
      ScanFlat(result, 0, start, count);
      return;
    }
    EnsureDictionary();
    result.Dictionary(entries.dictionary, Codes(start, count), count);
  }

  void Select(duckdb::Vector& result, idx_t start, idx_t vector_count,
              const duckdb::SelectionVector& sel, idx_t sel_count) {
    if (!full && sel_count * 4 < vector_count) {
      const auto& codes = Codes(start, vector_count, sel, sel_count);
      EnsureCodes(codes, sel_count);
      WriteCodes(result, 0, codes, sel_count);
      return;
    }
    EnsureDictionary();
    result.Dictionary(entries.dictionary,
                      Codes(start, vector_count, sel, sel_count), sel_count);
  }

  void Filter(duckdb::Vector& result, idx_t start, idx_t vector_count,
              duckdb::SelectionVector& sel, idx_t& sel_count,
              const duckdb::TableFilter& filter,
              duckdb::TableFilterState& filter_state) {
    const auto dict_count = entries.header.entry_count;
    if (!filter_result && !lazy_filter) {
      if (const auto ranges = ResolveFilter(filter)) {
        filter_result = duckdb::make_unsafe_uniq_array<bool>(dict_count);
        std::memset(filter_result.get(), 0, dict_count);
        idx_t matched = 0;
        for (const auto& [lo, hi] : *ranges) {
          std::memset(filter_result.get() + lo, 1, hi - lo);
          matched += hi - lo;
        }
        filter_match_count = matched;
      } else if (dict_count < kLazyFilterEntries ||
                 sel_count * 4 >= vector_count) {
        JudgeAll(filter_state);
      } else {
        lazy_filter = true;
        verdicts.assign(dict_count, 0);
      }
    }
    if (!lazy_filter && filter_match_count == 0) {
      sel_count = 0;
      return;
    }
    const auto& codes = Codes(start, vector_count);
    if (lazy_filter) {
      JudgeLazily(codes, sel, sel_count, filter_state);
    } else if (filter_match_count != dict_count) {
      const auto* matches = filter_result.get();
      idx_t kept = 0;
      while (kept < sel_count &&
             matches[codes.get_index(sel.get_index(kept))]) {
        ++kept;
      }
      for (idx_t i = kept + 1; i < sel_count; ++i) {
        const auto row = sel.get_index(i);
        if (matches[codes.get_index(row)]) {
          sel.set_index(kept++, row);
        }
      }
      sel_count = kept;
    }
    if (!full && filter_match_count * 4 >= dict_count) {
      EnsureDictionary();
    }
    if (full) {
      result.Dictionary(entries.dictionary, codes, vector_count);
      return;
    }
    ScanFiltered(result, codes, vector_count, sel, sel_count);
  }

  void ScanFiltered(duckdb::Vector& result,
                    const duckdb::SelectionVector& codes, idx_t count,
                    const duckdb::SelectionVector& rows, idx_t row_count) {
    EnsureEach(row_count,
               [&](idx_t i) { return codes.get_index(rows.get_index(i)); });
    entries.ReferenceHeap(result);
    auto writer = duckdb::FlatVector::Writer<string_t>(result, count);
    idx_t next = 0;
    for (idx_t row = 0; row < count; ++row) {
      if (next < row_count && rows.get_index(next) == row) {
        ++next;
        if (const auto code = codes.get_index(row); code != 0) {
          writer.WriteStringRef(entries.values[code]);
          continue;
        }
      }
      writer.WriteNull();
    }
  }

  void JudgeAll(duckdb::TableFilterState& filter_state) {
    const auto dict_count = entries.header.entry_count;
    EnsureDictionary();
    filter_result = duckdb::make_unsafe_uniq_array<bool>(dict_count);
    std::memset(filter_result.get(), 0, dict_count);
    duckdb::SelectionVector dict_sel;
    idx_t filter_count = dict_count;
    duckdb::ColumnSegment::FilterSelection(dict_sel, entries.dictionary->data,
                                           filter_state, dict_count,
                                           filter_count);
    for (idx_t i = 0; i < filter_count; ++i) {
      filter_result[dict_sel.get_index(i)] = true;
    }
    filter_match_count = filter_count;
    lazy_filter = false;
    verdicts = {};
  }

  void JudgeLazily(const duckdb::SelectionVector& codes,
                   duckdb::SelectionVector& rows, idx_t& count,
                   duckdb::TableFilterState& filter_state) {
    constexpr uint8_t kRejected = 1;
    constexpr uint8_t kAccepted = 2;
    constexpr uint8_t kPending = 3;
    pending.clear();
    for (idx_t i = 0; i < count; ++i) {
      const auto code = codes.get_index(rows.get_index(i));
      if (verdicts[code] == 0) {
        verdicts[code] = kPending;
        pending.emplace_back(static_cast<uint32_t>(code));
      }
    }
    if (!pending.empty()) {
      EnsureEach(pending.size(), [&](idx_t i) { return pending[i]; });
      duckdb::Vector candidates{entries.type, pending.size()};
      duckdb::StringVector::AddHeapReference(candidates,
                                             entries.dictionary->data);
      {
        auto writer =
          duckdb::FlatVector::Writer<string_t>(candidates, pending.size());
        for (const auto code : pending) {
          if (code == 0) {
            writer.WriteNull();
          } else {
            writer.WriteStringRef(entries.values[code]);
          }
        }
      }
      duckdb::SelectionVector kept;
      idx_t kept_count = pending.size();
      duckdb::ColumnSegment::FilterSelection(kept, candidates, filter_state,
                                             pending.size(), kept_count);
      for (const auto code : pending) {
        verdicts[code] = kRejected;
      }
      for (idx_t i = 0; i < kept_count; ++i) {
        verdicts[pending[kept.get_index(i)]] = kAccepted;
      }
      judged += pending.size();
    }
    idx_t out = 0;
    for (idx_t i = 0; i < count; ++i) {
      const auto row = rows.get_index(i);
      if (verdicts[codes.get_index(row)] == kAccepted) {
        rows.set_index(out++, row);
      }
    }
    count = out;
    if (judged * 2 >= entries.header.entry_count) {
      JudgeAll(filter_state);
    }
  }

  void ReserveCodes(idx_t count) {
    if (!code_sel || code_sel_size < count) {
      code_sel_size = count;
      code_sel = duckdb::make_buffer<duckdb::SelectionVector>(count);
    }
  }

  idx_t UnpackCodes(idx_t start, idx_t count) {
    const auto& h = entries.header;
    if (code_frames) {
      ReserveCodes(count);
      auto* codes = code_sel->data();
      code_frames->Read(start, count, codes);
      duckdb::sel_t max_code = 0;
      for (idx_t i = 0; i < count; ++i) {
        max_code = std::max(max_code, codes[i]);
      }
      SDB_ENSURE(max_code < h.entry_count, "col codec: corrupted row codes");
      return 0;
    }
    if (Rle()) {
      ReserveCodes(count);
      auto* codes = code_sel->data();
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
    auto* src =
      entries.base + h.off_codes + ((start - start_offset) * h.code_width) / 8;
    duckdb::BitpackingPrimitives::UnPackBuffer<duckdb::sel_t>(
      duckdb::data_ptr_cast(code_sel->data()), src, decode_count, h.code_width);
    const auto* codes = code_sel->data() + start_offset;
    duckdb::sel_t max_code = 0;
    for (idx_t i = 0; i < count; ++i) {
      max_code = std::max(max_code, codes[i]);
    }
    SDB_ENSURE(max_code < h.entry_count, "col codec: corrupted row codes");
    return start_offset;
  }

  EntryFrames<Shape::Dedup, Leaf> entries;
  duckdb::ObjectCache* cache = nullptr;
  std::string cache_key;
  std::atomic<bool>* touched = nullptr;
  bool full = false;
  bool cold_entries = false;
  uint64_t warmed_bytes = 0;
  duckdb::buffer_ptr<duckdb::SelectionVector> code_sel;
  idx_t code_sel_size = 0;
  std::vector<uint32_t> run_values;
  std::vector<uint32_t> run_ends;
  std::optional<FrameDecoder<uint32_t>> code_frames;
  duckdb::unsafe_unique_array<bool> filter_result;
  idx_t filter_match_count = 0;
  std::vector<uint8_t> verdicts;
  std::vector<uint32_t> pending;
  uint64_t judged = 0;
  bool lazy_filter = false;
};

}  // namespace irs::codecs::text
