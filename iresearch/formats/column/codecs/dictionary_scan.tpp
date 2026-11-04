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
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

template<typename Leaf>
struct DictionaryFetch final : duckdb::SegmentScanState {
  void Reset(duckdb::ColumnSegment& segment, data_ptr_t) {
    code_frames.reset();
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
    auto& numeric = Numeric(base);
    for (idx_t i = 0; i < count; ++i) {
      SDB_ENSURE(rows[i] < h.row_count, "col codec: row out of range");
      keep(i, numeric.Fetch(rows[i], i == 0 ? kNoRow : rows[i - 1],
                            i + 1 == count ? kNoRow : rows[i + 1]));
    }
    order.resize(kept);
  }

  IRS_FORCE_INLINE uint32_t ReadOne(data_ptr_t base, idx_t row) {
    const auto& h = frames.header;
    SDB_ENSURE(row < h.row_count, "col codec: row out of range");
    const auto code = Numeric(base).Fetch(row, kNoRow, kNoRow);
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

template<typename L>
struct DictionaryScan final : duckdb::SegmentScanState {
  using Leaf = L;
  using Fetch = DictionaryFetch<L>;
  static constexpr Shape kShape = Shape::Dedup;
  static constexpr auto kValidity =
    duckdb::CompressionValidity::NO_VALIDITY_REQUIRED;
  static constexpr uint8_t kUnjudged = 0;
  static constexpr uint8_t kRejected = 1;
  static constexpr uint8_t kAccepted = 2;
  static constexpr uint8_t kPending = 3;

  DictionaryScan(duckdb::ColumnSegment& segment, duckdb::BufferHandle handle)
    : DictionaryScan{segment, std::move(handle), CachedDictionary(segment)} {}

  DictionaryScan(duckdb::ColumnSegment& segment, duckdb::BufferHandle handle,
                 const duckdb::shared_ptr<DecodedDictionary>& cached)
    : entries{segment, std::move(handle), !cached},
      code_frames{
        entries.base + entries.header.off_codes,
        CodesHeader(entries.base, entries.header), nullptr,
        ReadContext::CacheSlot{
          .file_offset = entries.file_offset + entries.header.off_codes,
        }} {
    if (const auto& slot = CacheSlotOf(segment); slot.touched) {
      cache = slot.cache;
      cache_slot = slot;
    }
    if (cached) {
      AdoptDictionary(*cached);
    } else {
      entries.values[0] = string_t{};
      duckdb::FlatVector::ValidityMutable(entries.dictionary->data)
        .SetInvalid(0);
      full = entries.frames.empty();
    }
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
    if (cache && Touched(cache_slot)) {
      if (const auto cached = cache->Get<DecodedDictionary>(
            DictionaryCacheKey(cache_slot.file_id, cache_slot.file_offset))) {
        AdoptDictionary(*cached);
        return;
      }
    }
    entries.DecodeAll();
    Admit();
  }

  void Admit() {
    full = true;
    if (!cache || !SecondTouch(cache_slot)) {
      return;
    }
    const auto bytes = entries.CacheBytes();
    if (cache->GetCurrentMemoryRacy() + bytes <= cache->GetMaxMemoryRacy()) {
      cache->Put(
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
        if (children.empty() ||
            !duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*children[0])) {
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
            if constexpr (Sorted(Leaf::kCodec)) {
              return ResolveIn(children);
            } else {
              return std::nullopt;
            }
          default:
            return std::nullopt;
        }
      }
      case duckdb::ExpressionClass::BOUND_FUNCTION:
        if constexpr (Sorted(Leaf::kCodec)) {
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
    const auto constant =
      duckdb::ExpressionFilter::TryGetColumnConstantComparison(fn, comparison);
    const auto* value = constant ? StringConstant(*constant) : nullptr;
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
    UnpackCodes(start, count);
    return *code_sel;
  }

  const duckdb::SelectionVector& Codes(idx_t start, idx_t span,
                                       const duckdb::SelectionVector& rows,
                                       idx_t row_count) {
    UnpackCodes(start, span);
    auto* codes = code_sel->data();
    for (idx_t i = 0; i < row_count; ++i) {
      codes[i] = codes[rows.get_index(i)];
    }
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
    if (entries.Streams()) {
      Stream(result, result_offset, start, count);
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

  void ScanVector(duckdb::Vector& result, idx_t start, idx_t count) {
    if (count != STANDARD_VECTOR_SIZE || entries.Streams()) {
      ScanFlat(result, 0, start, count);
      return;
    }
    EnsureDictionary();
    result.Dictionary(entries.dictionary, Codes(start, count), count);
  }

  void Stream(duckdb::Vector& result, idx_t result_offset, idx_t start,
              idx_t count) {
    SDB_ASSERT(start >= streamed);
    streamed = start + count;
    const auto& codes = Codes(start, count);
    if (!full) {
      if constexpr (Sorted(Leaf::kCodec)) {
        EnsureDictionary();
      } else {
        EnsureThrough(batch_max);
      }
    }
    WriteCodes(result, result_offset, codes, count);
  }

  void EnsureThrough(uint32_t code) {
    if (code < entries.kFirstEntry) {
      return;
    }
    const auto last = entries.FrameOf(code);
    for (; next_frame <= last; ++next_frame) {
      if (!entries.decoded[next_frame]) {
        entries.DecodeFrame(next_frame);
      }
    }
    if (entries.Complete()) {
      Admit();
    }
  }

  void Select(duckdb::Vector& result, idx_t start, idx_t vector_count,
              const duckdb::SelectionVector& sel, idx_t sel_count) {
    SDB_ASSERT(!entries.Streams());
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
    SDB_ASSERT(!entries.Streams());
    const auto dict_count = entries.header.entry_count;
    if (verdicts.empty()) {
      if (const auto ranges = ResolveFilter(filter)) {
        verdicts.assign(dict_count, kRejected);
        idx_t matched = 0;
        for (const auto& [lo, hi] : *ranges) {
          std::memset(verdicts.data() + lo, kAccepted, hi - lo);
          matched += hi - lo;
        }
        filter_match_count = matched;
      } else if (dict_count < kLazyFilterEntries ||
                 sel_count * 4 >= vector_count) {
        JudgeAll(filter_state);
      } else {
        lazy_filter = true;
        verdicts.assign(dict_count, kUnjudged);
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
      const auto* matches = verdicts.data();
      idx_t kept = 0;
      while (kept < sel_count &&
             matches[codes.get_index(sel.get_index(kept))] == kAccepted) {
        ++kept;
      }
      for (idx_t i = kept + 1; i < sel_count; ++i) {
        const auto row = sel.get_index(i);
        if (matches[codes.get_index(row)] == kAccepted) {
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
    duckdb::SelectionVector dict_sel;
    idx_t filter_count = dict_count;
    duckdb::ColumnSegment::FilterSelection(dict_sel, entries.dictionary->data,
                                           filter_state, dict_count,
                                           filter_count);
    verdicts.assign(dict_count, kRejected);
    for (idx_t i = 0; i < filter_count; ++i) {
      verdicts[dict_sel.get_index(i)] = kAccepted;
    }
    filter_match_count = filter_count;
    lazy_filter = false;
  }

  void JudgeLazily(const duckdb::SelectionVector& codes,
                   duckdb::SelectionVector& rows, idx_t& count,
                   duckdb::TableFilterState& filter_state) {
    pending.clear();
    for (idx_t i = 0; i < count; ++i) {
      const auto code = codes.get_index(rows.get_index(i));
      if (verdicts[code] == kUnjudged) {
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

  void UnpackCodes(idx_t start, idx_t count) {
    ReserveCodes(count);
    auto* codes = code_sel->data();
    code_frames.Read(start, count, codes);
    duckdb::sel_t max_code = 0;
    for (idx_t i = 0; i < count; ++i) {
      max_code = std::max(max_code, codes[i]);
    }
    SDB_ENSURE(max_code < entries.header.entry_count,
               "col codec: corrupted row codes");
    batch_max = max_code;
  }

  EntryFrames<Shape::Dedup, Leaf> entries;
  duckdb::ObjectCache* cache = nullptr;
  ReadContext::CacheSlot cache_slot;
  duckdb::sel_t batch_max = 0;
  uint32_t next_frame = 0;
  idx_t streamed = 0;
  bool full = false;
  duckdb::buffer_ptr<duckdb::SelectionVector> code_sel;
  idx_t code_sel_size = 0;
  FrameDecoder<uint32_t> code_frames;
  idx_t filter_match_count = 0;
  std::vector<uint8_t> verdicts;
  std::vector<uint32_t> pending;
  uint64_t judged = 0;
  bool lazy_filter = false;
};

}  // namespace irs::codecs::text
