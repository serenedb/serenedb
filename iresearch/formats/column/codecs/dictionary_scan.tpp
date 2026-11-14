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
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

template<typename Leaf>
struct DictionaryFetch final : SegmentScanState {
  void Reset(const ColumnSegment& segment) {
    code_frames.reset();
    frames.decoded = CachedDictionary(segment.cache_slot);
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
    auto& numeric = Numeric(base);
    for (idx_t i = 0; i < count; ++i) {
      SDB_ENSURE(rows[i] < h.row_count, "col codec: row out of range");
      const uint32_t code =
        numeric.Fetch(rows[i], i + 1 == count ? kNoRow : rows[i + 1]);
      const auto at = offset + sel.get_index(i);
      if (code == 0) {
        validity.SetInvalid(at);
        continue;
      }
      SDB_ENSURE(code < h.entry_count, "col codec: corrupted row codes");
      out[kept++] = {.entry = code, .at = at};
    }
    order.resize(kept);
  }

  IRS_FORCE_INLINE uint32_t ReadOne(data_ptr_t base, idx_t row) {
    const auto& h = frames.header;
    SDB_ENSURE(row < h.row_count, "col codec: row out of range");
    const auto code = Numeric(base).Fetch(row, kNoRow);
    SDB_ENSURE(code == 0 || code < h.entry_count,
               "col codec: corrupted row codes");
    return code;
  }

  FrameDecoder<uint32_t>& Numeric(data_ptr_t base) {
    if (!code_frames) {
      code_frames.emplace(base + frames.header.off_codes,
                          CodesHeader(base, frames.header));
    }
    return *code_frames;
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

  FetchFrames<Leaf> frames;
  std::optional<FrameDecoder<uint32_t>> code_frames;
};

class FilterVerdicts {
 public:
  bool Empty() const noexcept { return _verdicts.empty(); }
  bool Lazy() const noexcept { return _lazy; }
  idx_t Matches() const noexcept { return _matches; }

  void Accept(const EntryRanges& ranges, uint32_t entries) {
    _verdicts.assign(entries, kRejected);
    _matches = 0;
    for (const auto& [lo, hi] : ranges) {
      std::memset(_verdicts.data() + lo, kAccepted, hi - lo);
      _matches += hi - lo;
    }
    _lazy = false;
  }

  void Accept(const duckdb::SelectionVector& accepted, idx_t count,
              uint32_t entries) {
    _verdicts.assign(entries, kRejected);
    for (idx_t i = 0; i < count; ++i) {
      _verdicts[accepted.get_index(i)] = kAccepted;
    }
    _matches = count;
    _lazy = false;
  }

  void Defer(uint32_t entries) {
    _verdicts.assign(entries, kUnjudged);
    _lazy = true;
  }

  std::span<const uint32_t> Unjudged(const duckdb::SelectionVector& codes,
                                     const duckdb::SelectionVector& rows,
                                     idx_t count) {
    _pending.clear();
    for (idx_t i = 0; i < count; ++i) {
      const auto code = codes.get_index(rows.get_index(i));
      if (_verdicts[code] == kUnjudged) {
        _verdicts[code] = kPending;
        _pending.emplace_back(static_cast<uint32_t>(code));
      }
    }
    return _pending;
  }

  void Judge(const duckdb::SelectionVector& kept, idx_t kept_count) {
    for (const auto code : _pending) {
      _verdicts[code] = kRejected;
    }
    for (idx_t i = 0; i < kept_count; ++i) {
      _verdicts[_pending[kept.get_index(i)]] = kAccepted;
    }
    _judged += _pending.size();
  }

  bool MostlyJudged(uint32_t entries) const noexcept {
    return _judged * 2 >= entries;
  }

  idx_t KeepAccepted(const duckdb::SelectionVector& codes,
                     duckdb::SelectionVector& rows, idx_t count) const {
    const auto* matches = _verdicts.data();
    idx_t kept = 0;
    while (kept < count &&
           matches[codes.get_index(rows.get_index(kept))] == kAccepted) {
      ++kept;
    }
    for (idx_t i = kept + 1; i < count; ++i) {
      const auto row = rows.get_index(i);
      if (matches[codes.get_index(row)] == kAccepted) {
        rows.set_index(kept++, row);
      }
    }
    return kept;
  }

 private:
  static constexpr uint8_t kUnjudged = 0;
  static constexpr uint8_t kRejected = 1;
  static constexpr uint8_t kAccepted = 2;
  static constexpr uint8_t kPending = 3;

  std::vector<uint8_t> _verdicts;
  std::vector<uint32_t> _pending;
  idx_t _matches = 0;
  uint64_t _judged = 0;
  bool _lazy = false;
};

template<typename Leaf>
struct DictionaryScan final : SegmentScanState {
  using Fetch = DictionaryFetch<Leaf>;
  static constexpr Shape kShape = Shape::Dedup;
  static constexpr auto kValidity = CompressionValidity::NoValidityRequired;

  DictionaryScan(const ColumnSegment& segment, duckdb::BufferHandle handle)
    : DictionaryScan{segment, std::move(handle),
                     CachedDictionary(segment.cache_slot)} {}

  DictionaryScan(const ColumnSegment& segment, duckdb::BufferHandle handle,
                 const duckdb::shared_ptr<DecodedDictionary>& cached)
    : entries{segment, std::move(handle), !cached},
      cache_slot{segment.cache_slot},
      code_frames{entries.base + entries.header.off_codes,
                  CodesHeader(entries.base, entries.header)} {
    if (cached) {
      AdoptDictionary(*cached);
      return;
    }
    entries.values[0] = string_t{};
    duckdb::FlatVector::ValidityMutable(entries.dictionary->data).SetInvalid(0);
    full = entries.frames.empty();
  }

  void AdoptDictionary(const DecodedDictionary& cached) {
    const auto& owner = cached.Dictionary();
    const auto count = entries.header.entry_count;
    duckdb::Vector view{entries.type,
                        duckdb::FlatVector::GetDataMutable(owner->data), count};
    auto& validity = duckdb::FlatVector::ValidityMutable(view);
    validity.Initialize(count);
    validity.SetInvalid(0);
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
    if (const auto cached = CachedDictionary(cache_slot)) {
      AdoptDictionary(*cached);
      return;
    }
    entries.DecodeAll();
    Admit();
  }

  void Admit() {
    full = true;
    if (!cache_slot.touched || !SecondTouch(cache_slot)) {
      return;
    }
    auto& cache = *cache_slot.cache;
    const auto bytes = entries.CacheBytes();
    if (cache.GetCurrentMemoryRacy() + bytes <= cache.GetMaxMemoryRacy()) {
      cache.Put(
        DictionaryCacheKey(cache_slot.file_id, cache_slot.file_offset),
        duckdb::make_shared_ptr<DecodedDictionary>(entries.dictionary, bytes));
    }
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
    return static_cast<uint32_t>(PartitionIndex(
      1, entries.header.entry_count,
      [&](size_t e) { return before(View(entries.values[e])); }));
  }

  uint32_t Lower(std::string_view key) {
    return Bound([&](std::string_view v) { return v < key; });
  }

  uint32_t Upper(std::string_view key) {
    return Bound([&](std::string_view v) { return v <= key; });
  }

  EntryRange EqualRange(std::string_view key) {
    return {Lower(key), Upper(key)};
  }

  bool Resolve(const duckdb::Expression& expr, EntryRanges& out) {
    const uint32_t n = entries.header.entry_count;
    switch (expr.GetExpressionClass()) {
      case duckdb::ExpressionClass::BOUND_OPERATOR: {
        const auto& op = expr.Cast<duckdb::BoundOperatorExpression>();
        const auto type = op.GetExpressionType();
        if ((type != duckdb::ExpressionType::OPERATOR_IS_NULL &&
             type != duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) ||
            !duckdb::ExpressionFilter::IsSimpleFilterColumnRef(
              *op.GetChildren()[0])) {
          return false;
        }
        out = type == duckdb::ExpressionType::OPERATOR_IS_NULL
                ? EntryRanges{{0, 1}}
                : Normalize({{1, n}});
        return true;
      }
      case duckdb::ExpressionClass::BOUND_FUNCTION:
        if constexpr (Sorted(Leaf::kCodec)) {
          return ResolveComparison(expr.Cast<duckdb::BoundFunctionExpression>(),
                                   out);
        } else {
          return false;
        }
      case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
        const auto& conjunction =
          expr.Cast<duckdb::BoundConjunctionExpression>();
        const auto kind = conjunction.GetExpressionType();
        const auto& children = conjunction.GetChildren();
        if (!Resolve(*children[0], out)) {
          return false;
        }
        EntryRanges ranges;
        for (size_t i = 1; i < children.size(); ++i) {
          if (!Resolve(*children[i], ranges)) {
            return false;
          }
          if (kind == duckdb::ExpressionType::CONJUNCTION_AND) {
            out = Intersect(out, ranges);
          } else {
            out.insert(out.end(), ranges.begin(), ranges.end());
            out = Normalize(std::move(out));
          }
        }
        return true;
      }
      default:
        return false;
    }
  }

  bool ResolveComparison(const duckdb::BoundFunctionExpression& fn,
                         EntryRanges& out) {
    const uint32_t n = entries.header.entry_count;
    auto comparison = fn.GetExpressionType();
    if (!duckdb::BoundComparisonExpression::IsComparison(comparison)) {
      return false;
    }
    const auto constant =
      duckdb::ExpressionFilter::TryGetColumnConstantComparison(fn, comparison);
    const auto* value = constant ? StringConstant(*constant) : nullptr;
    if (!value) {
      return false;
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
        return false;
    }
    if (value->IsNull()) {
      out.clear();
      return true;
    }
    const std::string_view key = duckdb::StringValue::Get(*value);
    switch (comparison) {
      case duckdb::ExpressionType::COMPARE_EQUAL:
        out = Normalize({EqualRange(key)});
        break;
      case duckdb::ExpressionType::COMPARE_NOTEQUAL: {
        const auto [lo, hi] = EqualRange(key);
        out = Normalize({{1, lo}, {hi, n}});
        break;
      }
      case duckdb::ExpressionType::COMPARE_LESSTHAN:
        out = Normalize({{1, Lower(key)}});
        break;
      case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
        out = Normalize({{1, Upper(key)}});
        break;
      case duckdb::ExpressionType::COMPARE_GREATERTHAN:
        out = Normalize({{Upper(key), n}});
        break;
      default:
        out = Normalize({{Lower(key), n}});
        break;
    }
    return true;
  }

  const duckdb::SelectionVector& Codes(idx_t start, idx_t count) {
    UnpackCodes(start, count);
    return *code_sel;
  }

  const duckdb::SelectionVector& Codes(idx_t start, idx_t span,
                                       const duckdb::SelectionVector& rows,
                                       idx_t row_count) {
    UnpackCodes(start, span);
    code_sel->SliceInPlace(rows, row_count);
    return *code_sel;
  }

  void WriteCodes(duckdb::Vector& result, idx_t result_offset,
                  const duckdb::SelectionVector& codes, idx_t count) {
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

  void Select(duckdb::Vector& result, idx_t result_offset, idx_t start,
              idx_t vector_count, const duckdb::SelectionVector& sel,
              idx_t sel_count, duckdb::ScanVectorType scan_type) {
    if (scan_type == duckdb::ScanVectorType::SCAN_FLAT_VECTOR ||
        (!full && sel_count * 4 < vector_count)) {
      const auto& codes = Codes(start, vector_count, sel, sel_count);
      EnsureCodes(codes, sel_count);
      WriteCodes(result, result_offset, codes, sel_count);
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
    const bool dense = sel_count * 4 >= vector_count;
    if (verdicts.Empty()) {
      if (EntryRanges ranges;
          Resolve(*filter.Cast<duckdb::ExpressionFilter>().expr, ranges)) {
        verdicts.Accept(ranges, dict_count);
      } else if (dict_count < kLazyFilterEntries || dense) {
        JudgeAll(filter_state);
      } else {
        verdicts.Defer(dict_count);
      }
    } else if (verdicts.Lazy() && dense) {
      JudgeAll(filter_state);
    }
    if (!verdicts.Lazy() && verdicts.Matches() == 0) {
      sel_count = 0;
      return;
    }
    const auto& codes = Codes(start, vector_count);
    if (verdicts.Lazy()) {
      JudgeLazily(codes, sel, sel_count, filter_state);
    } else if (verdicts.Matches() != dict_count) {
      sel_count = verdicts.KeepAccepted(codes, sel, sel_count);
    }
    if (!full && verdicts.Matches() * 4 >= dict_count) {
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
    duckdb::SelectionVector dict_sel;
    idx_t filter_count = dict_count;
    duckdb::ColumnSegment::FilterSelection(dict_sel, entries.dictionary->data,
                                           filter_state, dict_count,
                                           filter_count);
    verdicts.Accept(dict_sel, filter_count, dict_count);
  }

  void JudgeLazily(const duckdb::SelectionVector& codes,
                   duckdb::SelectionVector& rows, idx_t& count,
                   duckdb::TableFilterState& filter_state) {
    if (const auto pending = verdicts.Unjudged(codes, rows, count);
        !pending.empty()) {
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
      verdicts.Judge(kept, kept_count);
    }
    count = verdicts.KeepAccepted(codes, rows, count);
    if (verdicts.MostlyJudged(entries.header.entry_count)) {
      JudgeAll(filter_state);
    }
  }

  void UnpackCodes(idx_t start, idx_t count) {
    if (!code_sel || code_sel_size < count) {
      code_sel_size = count;
      code_sel = duckdb::make_buffer<duckdb::SelectionVector>(count);
    }
    auto* codes = code_sel->data();
    code_frames.Read(start, count, codes);
    duckdb::sel_t max_code = 0;
    for (idx_t i = 0; i < count; ++i) {
      max_code = std::max(max_code, codes[i]);
    }
    SDB_ENSURE(max_code < entries.header.entry_count,
               "col codec: corrupted row codes");
  }

  EntryFrames<Shape::Dedup, Leaf> entries;
  ReadContext::CacheSlot cache_slot;
  bool full = false;
  duckdb::buffer_ptr<duckdb::SelectionVector> code_sel;
  idx_t code_sel_size = 0;
  FrameDecoder<uint32_t> code_frames;
  FilterVerdicts verdicts;
};

}  // namespace irs::codecs::text
