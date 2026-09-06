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

#include <array>
#include <deque>
#include <optional>
#include <span>

#include "basics/containers/flat_hash_map.h"
#include "basics/log.h"
#include "basics/noncopyable.hpp"
#include "iresearch/analysis/numeric_terms.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/inverter/posting_log.hpp"
#include "iresearch/index/inverter/term_dictionary.hpp"
#include "iresearch/index/typed_terms.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {

struct FlushState;
struct BasicTermReader;

namespace burst_trie {

class FieldWriter;
}

struct InverterMemory {
  duckdb::Allocator& allocator;
  IResourceManager& rm;
};

class FieldInverter : util::Noncopyable {
 public:
  using TermIds = std::span<uint32_t, TokenBatch::kCapacity>;

  FieldInverter(field_id id, duckdb::ArenaAllocator& arena, TermIds term_ids,
                IResourceManager& rm, IndexFeatures index_features,
                ColWriter* col_writer, const IndexFieldOptions* field_options)
    : _term_ids{term_ids},
      _dict{arena, rm},
      _inline_docs{ManagedTypedAllocator<doc_id_t>{rm}},
      _log{MakePostingLog(arena, rm, LayoutFromFeatures(index_features))},
      _meta{id, index_features} {
    if (!col_writer || !IsSubsetOf(IndexFeatures::Norm, index_features)) {
      return;
    }
    SDB_ENSURE(field_options,
               "Norm-featured field requires per-field index options");
    const auto norm_id = field_options->GetNormColumnId(id);
    SDB_ENSURE(field_limits::valid(norm_id),
               "GetNormColumnId must return a valid id for field ", id);
    _col_writer = col_writer;
    _norm_row_group_size = field_options->row_group_size;
    _meta.norm = norm_id;
  }

  const auto& Meta() const noexcept { return _meta; }
  doc_id_t LastDoc() const noexcept { return _state.last_doc; }
  const TermDictionary& Dictionary() const noexcept { return _dict; }
  std::span<const doc_id_t> InlineDocs() const noexcept {
    return {_inline_docs.data(), _inline_docs.size()};
  }

  void Configure(const TokenTraits& traits) noexcept {
    _one_to_one = traits.unique;
    _dense_pos = !traits.explicit_pos;
  }
  bool UniqueTerms() const noexcept { return _unique_terms; }
  TokenLayout Layout() const noexcept {
    return static_cast<TokenLayout>(_log.index());
  }
  const PostingLogBase& Log() const noexcept {
    return std::visit(
      [](const auto& log) -> const PostingLogBase& { return log; }, _log);
  }
  template<typename Visitor>
  decltype(auto) VisitLog(this auto&& self, Visitor&& visitor) {
    return std::visit(std::forward<Visitor>(visitor), self._log);
  }

  size_t Memory() const noexcept {
    return _dict.Memory() + _inline_docs.capacity() * sizeof(doc_id_t) +
           VisitLog([](const auto& log) { return log.BookkeepingMemory(); });
  }

  bool InvertPrimaryKeyBlock(std::span<const duckdb::string_t> values,
                             doc_id_t first_doc) {
    MarkUniqueTerms();
    InvertUniqueKeywords(values, first_doc);
    return true;
  }

  bool InvertBoolBlock(const duckdb::UnifiedVectorFormat& fmt, uint32_t count,
                       doc_id_t first_doc) {
    SDB_ASSERT(!_unique_terms);
    const auto* data = duckdb::UnifiedVectorFormat::GetData<bool>(fmt);
    const duckdb::string_t terms[2] = {BoolTerm(false), BoolTerm(true)};
    const uint32_t ids[2] = {_dict.Insert(terms[0]), _dict.Insert(terms[1])};
    return VisitLog([&](auto& log) {
      return analysis::ForEachValidRow(
        fmt, count, [&](uint32_t r, uint32_t idx) {
          const bool value = data[idx];
          return PushKeyword(log, first_doc + r, terms[value].GetSize(),
                             ids[value]);
        });
    });
  }

  template<typename ValueAt, typename P = std::remove_cvref_t<
                               std::invoke_result_t<ValueAt&, duckdb::idx_t>>>
  bool InvertNumericBlock(const duckdb::UnifiedVectorFormat& fmt, uint32_t n,
                          doc_id_t first_doc, ValueAt&& value_at) {
    const bool identity = analysis::IsIdentitySel(fmt);
    return analysis::ForEachValidRun(fmt, n, [&](uint32_t r0, uint32_t len) {
      const auto doc_at = [&](size_t k) noexcept {
        return first_doc + r0 + static_cast<doc_id_t>(k);
      };
      if (identity) {
        return InvertNumericImpl<P>(
          len, [&](size_t j) { return value_at(r0 + j); }, doc_at);
      }
      return InvertNumericImpl<P>(
        len, [&](size_t j) { return value_at(fmt.sel->get_index(r0 + j)); },
        doc_at);
    });
  }

  template<typename P, typename ForEach>
  bool InvertNumerics(ForEach&& for_each) {
    constexpr size_t kMaxValues = TokenBatch::kCapacity / NumericTermCount<P>();
    P vals[kMaxValues];
    doc_id_t docs[kMaxValues];
    size_t n = 0;
    bool ok = true;
    const auto flush = [&] {
      ok = InvertNumericImpl<P>(
        n, [&](size_t j) { return vals[j]; },
        [&](size_t k) noexcept { return docs[k]; });
      n = 0;
    };
    for_each([&](P value, doc_id_t doc) {
      if (!ok) [[unlikely]] {
        return;
      }
      vals[n] = value;
      docs[n] = doc;
      if (++n == kMaxValues) {
        flush();
      }
    });
    if (n != 0) {
      flush();
    }
    return ok;
  }

  bool InvertNullBlock(const duckdb::UnifiedVectorFormat& fmt, uint32_t count,
                       doc_id_t first_doc) {
    const auto id = _dict.Insert(NullTerm());
    return VisitLog([&](auto& log) {
      return analysis::ForEachInvalidRow(fmt, count, [&](uint32_t i) {
        return PushKeyword(log, first_doc + i, 0, id);
      });
    });
  }

  bool InvertNullBlock(uint32_t count, doc_id_t first_doc) {
    const auto id = _dict.Insert(NullTerm());
    return VisitLog([&](auto& log) {
      for (uint32_t i = 0; i < count; ++i) {
        if (!PushKeyword(log, first_doc + i, 0, id)) [[unlikely]] {
          return false;
        }
      }
      return true;
    });
  }

  bool InvertKeywordBlock(const duckdb::UnifiedVectorFormat& fmt,
                          uint32_t count, doc_id_t first_doc) {
    const auto* data =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
    if (analysis::IsIdentitySel(fmt)) {
      if (fmt.validity.CheckAllValid(count)) {
        return InvertDenseKeywordBlock({data, count}, first_doc);
      }
      return analysis::ForEachValidRun(
        fmt, count, [&](uint32_t run, uint32_t len) {
          return InvertDenseKeywordBlock(std::span{data + run, len},
                                         first_doc + run);
        });
    }
    SDB_ASSERT(!_unique_terms);
    return VisitLog([&](auto& log) {
      return analysis::ForEachValidRow(
        fmt, count, [&](uint32_t i, uint32_t idx) {
          const auto& value = data[idx];
          const auto id = _dict.Insert(value);
          return PushKeyword(log, first_doc + i, value.GetSize(), id);
        });
    });
  }

  template<typename ForEach>
  bool InvertKeywords(ForEach&& for_each) {
    SDB_ASSERT(!_unique_terms);
    return VisitLog([&](auto& log) {
      bool ok = true;
      for_each([&](duckdb::string_t value, doc_id_t doc) {
        if (!ok) [[unlikely]] {
          return;
        }
        const auto id = _dict.Insert(value);
        ok = PushKeyword(log, doc, value.GetSize(), id);
      });
      return ok;
    });
  }

  bool InvertBlock(TokenBatch& batch, DocRuns runs) {
    return VisitLog([&](auto& log) {
      auto* ids = ResolveTerms(batch.terms, batch.count);

      const bool first_continues = _state.value_open;
      if (_one_to_one && !first_continues && !runs.tail_open &&
          runs.size() == batch.count) {
        return InvertOneToOne(log, batch, runs, ids);
      }
      uint32_t tok = 0;
      for (size_t r = 0; r < runs.size(); ++r) {
        const auto& run = runs[r];
        if (!PushDoc(log, run.doc, batch, tok, run.ntokens, ids + tok,
                     r == 0 ? !first_continues : true)) [[unlikely]] {
          return false;
        }
        tok += run.ntokens;
      }
      if (!runs.empty() || runs.tail_open) {
        _state.value_open = runs.tail_open;
      }
      SDB_ASSERT(tok == batch.count);
      return true;
    });
  }

 private:
  friend class FieldsInverter;

  void ReserveTerms(size_t expected_terms, bool unique) {
    _dict.Reserve(expected_terms);
    if (!unique) {
      _dict.ReserveMap(expected_terms);
    }
  }

  void MarkUniqueTerms() {
    SDB_ASSERT(Layout() == TokenLayout::Terms);
    SDB_ASSERT(!_col_writer);
    SDB_ASSERT(!_dict.Size() || _unique_terms);
    _unique_terms = true;
  }

  void AppendInlineDoc(uint32_t id, doc_id_t doc) {
    SDB_ASSERT(id == _inline_docs.size());
    _inline_docs.push_back(doc);
  }

  template<typename Log>
  IRS_NO_INLINE bool InvertOneToOne(Log& log, TokenBatch& batch, DocRuns runs,
                                    const uint32_t* ids) {
    for (uint32_t i = 0, n = batch.count; i < n; ++i) {
      SDB_ASSERT(runs[i].ntokens == 1);
      SDB_ASSERT(i == 0 || runs[i].doc >= runs[i - 1].doc);
      if (!PushDoc(log, runs[i].doc, batch, i, 1, ids + i, true)) [[unlikely]] {
        return false;
      }
    }
    return true;
  }

  template<typename Log>
  IRS_FORCE_INLINE bool PushDoc(Log& log, doc_id_t id, TokenBatch& batch,
                                uint32_t base, uint32_t n, const uint32_t* ids,
                                bool value_start) {
    SDB_ASSERT(id < doc_limits::eof());
    SDB_ASSERT(base + n <= batch.count);
    if constexpr (Log::kLayout == TokenLayout::Terms) {
      return PushDocTerms(log, id, ids, n);
    } else {
      return PushDocRun(log, id, batch, base, n, ids, value_start);
    }
  }

  template<typename Log>
  IRS_FORCE_INLINE bool PushDocTerms(Log& log, doc_id_t id, const uint32_t* ids,
                                     uint32_t n) {
    static_assert(Log::kLayout == TokenLayout::Terms);
    Reset<TokenLayout::Terms>(id);
    if (!n) [[unlikely]] {
      return true;
    }
    if (!CheckDocBudget(id, n)) [[unlikely]] {
      return false;
    }
    _state.stats.len += n;
    log.PushBatch(id, {ids, n});
    return true;
  }

  struct PosChecks {
    uint32_t pos_base;
    uint32_t last_pos;
  };

  template<typename Log>
  bool PushDocRun(Log& log, doc_id_t id, TokenBatch& batch, uint32_t base,
                  uint32_t n, const uint32_t* ids, bool value_start) {
    Reset<Log::kLayout>(id);

    if (!n) [[unlikely]] {
      return true;
    }

    if (value_start) {
      _state.CaptureValueBases<Log::kLayout>();
    }

    const auto checks = ValidatePos(batch, base, n);
    if (!checks) [[unlikely]] {
      return false;
    }
    if constexpr (Log::kLayout == TokenLayout::TermsPosOffs) {
      if (!ValidateOffs(batch, base, n)) [[unlikely]] {
        return false;
      }
    }
    if (!CheckDocBudget(id, n)) [[unlikely]] {
      return false;
    }

    CommitRun(log, id, ids, batch, base, n, *checks);
    return true;
  }

  IRS_FORCE_INLINE std::optional<PosChecks> ValidatePos(const TokenBatch& batch,
                                                        uint32_t base,
                                                        uint32_t count) const {
    const auto* pos_arr = batch.pos + base;
    const bool dense = _dense_pos;
    const uint32_t pos_base = dense ? _state.last_pos : _state.value_pos;
    uint32_t last_pos;
    if (dense) {
      last_pos = _state.last_pos + count;
      if (last_pos < _state.last_pos || last_pos >= pos_limits::eof())
        [[unlikely]] {
        SDB_ERROR(IRESEARCH, "invalid position in field '", _meta.id, "'");
        return std::nullopt;
      }
    } else {
      bool monotonic = pos_base + pos_arr[0] >= _state.last_pos;
      for (uint32_t i = 1; i < count; ++i) {
        monotonic &= pos_arr[i] >= pos_arr[i - 1];
      }
      last_pos = pos_base + pos_arr[count - 1];
      if (!monotonic || last_pos < pos_base || last_pos >= pos_limits::eof())
        [[unlikely]] {
        SDB_ERROR(IRESEARCH, "invalid position in field '", _meta.id, "'");
        return std::nullopt;
      }
    }
    return PosChecks{pos_base, last_pos};
  }

  IRS_FORCE_INLINE bool ValidateOffs(const TokenBatch& batch, uint32_t base,
                                     uint32_t count) const {
    const auto* start = batch.offs_start + base;
    const auto* end = batch.offs_end + base;
    bool valid =
      uint64_t{_state.value_offs} + start[0] >= _state.last_start_offs &&
      end[0] >= start[0];
    uint32_t max_end = end[0];
    for (uint32_t i = 1; i < count; ++i) {
      valid &= start[i] >= start[i - 1];
      valid &= end[i] >= start[i];
      max_end = std::max(max_end, end[i]);
    }
    valid &= uint64_t{_state.value_offs} + max_end <=
             std::numeric_limits<uint32_t>::max();
    if (!valid) [[unlikely]] {
      SDB_ERROR(IRESEARCH, "invalid offset in field '", _meta.id, "'");
      return false;
    }
    return true;
  }

  IRS_FORCE_INLINE void AdvanceOffs(uint32_t start, uint32_t end) {
    _state.last_start_offs = start;
    _state.offs = end;
  }

  template<typename Log>
  IRS_FORCE_INLINE void CommitRun(Log& log, doc_id_t id, const uint32_t* ids,
                                  const TokenBatch& batch, uint32_t base,
                                  uint32_t count, const PosChecks& checks) {
    const auto* pos_arr = batch.pos + base;
    if constexpr (Log::kLayout == TokenLayout::TermsPos) {
      log.PushBatch(id, {ids, count}, _dense_pos, {pos_arr, count},
                    checks.pos_base);
    } else {
      log.PushBatch(id, {ids, count}, _dense_pos, {pos_arr, count},
                    checks.pos_base, {batch.offs_start + base, count},
                    {batch.offs_end + base, count}, _state.value_offs);
      AdvanceOffs(_state.value_offs + batch.offs_start[base + count - 1],
                  _state.value_offs + batch.offs_end[base + count - 1]);
    }
    _state.AdvancePos(checks.last_pos, count);
  }

  IRS_FORCE_INLINE bool CheckDocBudget(doc_id_t doc, uint64_t n) const {
    if (_state.stats.len + n > std::numeric_limits<int32_t>::max())
      [[unlikely]] {
      SDB_ERROR(IRESEARCH, "too many tokens in field: ", _meta.id,
                ", document: ", doc);
      return false;
    }
    return true;
  }

  const uint32_t* ResolveTerms(const duckdb::string_t* terms, size_t n) {
    _dict.Insert(std::span{terms, n}, _term_ids);
    return _term_ids.data();
  }

  IRS_FORCE_INLINE void InvertUniqueKeywords(
    std::span<const duckdb::string_t> values, doc_id_t first_doc) {
    SDB_ASSERT(Layout() == TokenLayout::Terms);
    SDB_ASSERT(!_col_writer);
    for (size_t k = 0; k < values.size(); ++k) {
      const auto doc = first_doc + static_cast<doc_id_t>(k);
      SDB_ASSERT(doc < doc_limits::eof());
      SDB_ASSERT(doc >= _state.last_doc);
      AppendInlineDoc(_dict.AppendUnique(values[k]), doc);
    }
    if (!values.empty()) {
      Reset(first_doc + static_cast<doc_id_t>(values.size() - 1));
      _state.AssumeKeywordTail();
    }
  }

  bool InvertDenseKeywordBlock(std::span<const duckdb::string_t> values,
                               doc_id_t first_doc) {
    SDB_ASSERT(!_unique_terms);
    return VisitLog([&]<typename LogT>(LogT& log) {
      for (size_t base = 0; base < values.size();
           base += TokenBatch::kCapacity) {
        const auto n = std::min(values.size() - base, TokenBatch::kCapacity);
        auto* const ids = ResolveTerms(values.data() + base, n);
        if constexpr (LogT::kLayout == TokenLayout::Terms) {
          if (!_col_writer) {
            CaptureKeywordTerms(log, ids, first_doc, base, n);
            continue;
          }
        }
        for (size_t j = 0; j < n; ++j) {
          const auto k = base + j;
          if (!PushKeyword(log, first_doc + static_cast<doc_id_t>(k),
                           values[k].GetSize(), ids[j])) [[unlikely]] {
            return false;
          }
        }
      }
      return true;
    });
  }

  template<typename P, typename ValueAt, typename DocAt>
  bool InvertNumericImpl(size_t n, ValueAt&& value_at, DocAt doc_at) {
    constexpr uint32_t kTerms = NumericTermCount<P>();
    constexpr size_t kMaxValues = TokenBatch::kCapacity / kTerms;
    SDB_ASSERT(!_state.value_open);
    SDB_ASSERT(Layout() == TokenLayout::Terms);
    auto& log = *std::get_if<PostingLog<TokenLayout::Terms>>(&_log);
    duckdb::string_t terms[kMaxValues * kTerms];
    for (size_t base = 0; base < n;) {
      const auto m =
        static_cast<uint32_t>(std::min<size_t>(kMaxValues, n - base));
      AppendNumericTermsBlock<P>(terms, m,
                                 [&](size_t j) { return value_at(base + j); });
      auto* const ids = ResolveTerms(terms, m * kTerms);
      uint32_t tok = 0;
      for (uint32_t k = 0; k < m; ++k) {
        if (!PushDocTerms(log, doc_at(base + k), ids + tok, kTerms))
          [[unlikely]] {
          return false;
        }
        tok += kTerms;
      }
      base += m;
    }
    return true;
  }

  template<typename Log>
  void CaptureKeywordTerms(Log& log, const uint32_t* ids, doc_id_t first_doc,
                           size_t base, size_t n) {
    static_assert(Log::kLayout == TokenLayout::Terms);
    SDB_ASSERT(!_col_writer);
    for (size_t j = 0; j < n; ++j) {
      const auto doc = first_doc + static_cast<doc_id_t>(base + j);
      SDB_ASSERT(doc < doc_limits::eof());
      SDB_ASSERT(doc >= _state.last_doc);
      log.PushOne(doc, ids[j]);
    }
    Reset(first_doc + static_cast<doc_id_t>(base + n - 1));
    _state.AssumeKeywordTail();
  }

  template<typename Log>
  bool PushKeyword(Log& log, doc_id_t id, uint32_t value_size,
                   uint32_t term_id) {
    SDB_ASSERT(id < doc_limits::eof());

    Reset<Log::kLayout>(id);

    if (!CheckDocBudget(id, 1)) [[unlikely]] {
      return false;
    }

    if constexpr (Log::kLayout == TokenLayout::Terms) {
      log.PushOne(id, term_id);
      ++_state.stats.len;
    } else {
      const uint32_t pos = _state.last_pos + 1;
      if (pos >= pos_limits::eof()) [[unlikely]] {
        SDB_ERROR(IRESEARCH, "invalid position ", pos,
                  " >= ", pos_limits::eof(), " in field '", _meta.id, "'");
        return false;
      }
      if constexpr (Log::kLayout == TokenLayout::TermsPos) {
        log.PushOne(id, term_id, pos);
      } else {
        log.PushOne(id, term_id, pos, _state.offs, _state.offs + value_size);
        AdvanceOffs(_state.offs, _state.offs + value_size);
      }
      _state.AdvancePos(pos, 1);
    }
    return true;
  }

  template<TokenLayout kLayout = TokenLayout::TermsPosOffs>
  void Reset(doc_id_t doc_id) {
    if (doc_id == _state.last_doc) {
      return;
    }
    FinalizeNorm();
    _state.ResetDoc<kLayout>(doc_id);
  }

  void FinalizeNorm() {
    if (!_col_writer || !doc_limits::valid(_state.last_doc)) {
      return;
    }
    if (!_norm_writer) {
      _norm_writer =
        &_col_writer->OpenNormColumn(_meta.norm, _norm_row_group_size);
    }
    _norm_writer->Append(
      static_cast<uint64_t>(_state.last_doc) - doc_limits::min(),
      _state.stats.len);
  }

  struct DocState {
    doc_id_t last_doc{doc_limits::invalid()};
    FieldStats stats;
    uint32_t last_pos{0};
    uint32_t offs{0};
    uint32_t last_start_offs{0};
    uint32_t value_pos{0};
    uint32_t value_offs{0};
    bool value_open{false};

    template<TokenLayout kLayout>
    void ResetDoc(doc_id_t doc) noexcept {
      stats = {};
      last_doc = doc;
      if constexpr (kLayout != TokenLayout::Terms) {
        last_pos = 0;
      }
      if constexpr (kLayout == TokenLayout::TermsPosOffs) {
        offs = 0;
        last_start_offs = 0;
      }
    }

    template<TokenLayout kLayout>
    void CaptureValueBases() noexcept {
      value_pos = last_pos;
      if constexpr (kLayout == TokenLayout::TermsPosOffs) {
        value_offs = offs;
      }
    }

    void AdvancePos(uint32_t last, uint32_t count) noexcept {
      stats.len += count;
      last_pos = last;
    }

    void AssumeKeywordTail() noexcept {
      last_pos = 1;
      stats.len = 1;
    }
  };

  TermIds _term_ids;
  TermDictionary _dict;
  ManagedVector<doc_id_t> _inline_docs;
  PostingLogVariant _log;
  ColWriter* _col_writer = nullptr;
  NormColumnWriter* _norm_writer = nullptr;
  uint32_t _norm_row_group_size = 0;
  FieldMeta _meta;
  bool _one_to_one = false;
  bool _unique_terms = false;
  bool _dense_pos = true;
  DocState _state;
};

class FieldsInverter : util::Noncopyable {
 public:
  explicit FieldsInverter(InverterMemory mem)
    : _mem{mem},
      _arena{mem.allocator},
      _fields{ManagedTypedAllocator<FieldInverter>{mem.rm}} {}

  void SetColWriter(ColWriter* w) noexcept { _col_writer = w; }
  void SetFieldOptions(const IndexFieldOptions* field_options) noexcept {
    _field_options = field_options;
  }
  duckdb::Allocator& Allocator() const noexcept { return _mem.allocator; }

  FieldInverter* Emplace(field_id id, IndexFeatures index_features) {
    auto& slot = _fields_map.try_emplace(id).first->second;
    if (slot.field) {
      return slot.field;
    }

    auto& field =
      _fields.emplace_back(id, _arena, _term_ids, _mem.rm, index_features,
                           _col_writer, _field_options);
    slot.field = &field;
    if (slot.last_terms) {
      field.ReserveTerms(slot.last_terms, slot.unique);
    }
    return slot.field;
  }

  void FinalizeNorms() {
    for (auto& field : _fields) {
      field.FinalizeNorm();
    }
  }

  void Flush(burst_trie::FieldWriter& fw, FlushState& state,
             std::span<const BasicTermReader* const> extra);

  size_t MemoryActive() const noexcept {
    return _arena.SizeInBytes() + FieldsMemory();
  }

  size_t MemoryReserved() const noexcept {
    return _arena.AllocationSize() + FieldsMemory();
  }

  void Reset() noexcept {
    for (auto& [id, slot] : _fields_map) {
      if (slot.field) {
        if (const auto n = slot.field->Dictionary().Size()) {
          slot.last_terms = static_cast<uint32_t>(n);
          slot.unique = slot.field->UniqueTerms();
        }
        slot.field = nullptr;
      }
    }
    _fields.clear();
    _arena.Reset();
  }

 private:
  struct FieldSlot {
    FieldInverter* field = nullptr;
    uint32_t last_terms = 0;
    bool unique = false;
  };
  using FieldsMap = sdb::containers::FlatHashMap<field_id, FieldSlot>;

  size_t FieldsMemory() const noexcept {
    size_t size = _fields.size() * sizeof(FieldsMap::value_type) +
                  _fields.size() * sizeof(FieldInverter);
    for (const auto& field : _fields) {
      size += field.Memory();
    }
    return size;
  }

  InverterMemory _mem;
  duckdb::ArenaAllocator _arena;
  std::array<uint32_t, TokenBatch::kCapacity> _term_ids;
  std::deque<FieldInverter, ManagedTypedAllocator<FieldInverter>> _fields;
  FieldsMap _fields_map;
  ColWriter* _col_writer = nullptr;
  const IndexFieldOptions* _field_options = nullptr;
};

}  // namespace irs
