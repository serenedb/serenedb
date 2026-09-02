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

namespace irs {

struct FlushState;
struct BasicTermReader;
struct ScatterScratch;

namespace burst_trie {

class FieldWriter;
}

// Chunk source for inversion containers. `allocator` should be the
// BufferAllocator of the owning database so inversion memory participates in
// the global memory budget; DefaultAllocator is the unaccounted fallback for
// standalone tests.
struct InverterMemory {
  duckdb::Allocator& allocator;
  IResourceManager& rm;

  static InverterMemory Default() noexcept {
    return {duckdb::Allocator::DefaultAllocator(), IResourceManager::gNoop};
  }
};

// Term-resolution scratch (hashes + resolved ids for one batch), owned by
// the writer and shared by all its fields.
struct ResolveScratch {
  uint64_t hashes[TokenBatch::kCapacity];
  uint32_t ids[TokenBatch::kCapacity];
};

// One inverter per field. Ingest entries, by column class (level-1 routing):
//   PK blocks (unique terms)      -> InvertPrimaryKeyBlock (ramp)
//   bool columns                  -> InvertBoolBlock (UVF, lazy term ids)
//   numeric columns               -> InvertNumericBlock (UVF: fused
//                                      encode+resolve, valid-run split)
//                                    / InvertNumerics (streamed pairs,
//                                      kernel-staged)
//   null docs                     -> InvertNullBlock (UVF: invalid rows
//                                      / all-null doc ramp); staged null
//                                      pairs ride InvertKeywords
//   verbatim keyword blocks       -> InvertKeywordBlock (UVF: dense valid
//                                      runs, or a sel walk under dictionary
//                                      encoding)
//                                    / InvertKeywords (streamed pairs)
//   analyzed columns (runs)       -> InvertBlock(batch, runs)
// Everything else in this class is machinery below these entries.
class FieldInverter : util::Noncopyable {
 public:
  struct NormSpec {
    ColWriter* col_writer = nullptr;
    field_id norm_id = field_limits::invalid();
    uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
  };

  FieldInverter(field_id id, duckdb::ArenaAllocator& arena,
                ResolveScratch& resolve, IResourceManager& rm,
                IndexFeatures index_features)
    : FieldInverter{id, arena, resolve, rm, index_features, NormSpec{}} {}

  FieldInverter(field_id id, duckdb::ArenaAllocator& arena,
                ResolveScratch& resolve, IResourceManager& rm,
                IndexFeatures index_features, NormSpec norms)
    : _resolve{&resolve},
      _dict{arena, rm},
      _inline_docs{ManagedTypedAllocator<doc_id_t>{rm}},
      _log{MakePostingLog(arena, rm, LayoutFromFeatures(index_features))},
      _meta{id, index_features & (~IndexFeatures::Offs)},
      _requested_features{index_features} {
    if (IsSubsetOf(IndexFeatures::Norm, index_features) && norms.col_writer &&
        field_limits::valid(norms.norm_id)) {
      _col_writer = norms.col_writer;
      _norm_row_group_size = norms.row_group_size;
      _meta.norm = norms.norm_id;
    }
  }

  const auto& Meta() const noexcept { return _meta; }
  doc_id_t LastDoc() const noexcept { return _state.last_doc; }
  const auto& Stats() const noexcept { return _state.stats; }
  IndexFeatures RequestedFeatures() const noexcept {
    return _requested_features;
  }
  auto& Dictionary(this auto& self) noexcept { return self._dict; }
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
    return _dict.Memory() +
           _inline_docs.capacity() *
             sizeof(decltype(_inline_docs)::value_type) +
           VisitLog([](const auto& log) { return log.BookkeepingMemory(); });
  }

  // The PK shape: terms are one-per-doc and (almost always) distinct, so
  // interning is append-only -- keyword blocks skip the hash/probe entirely
  // and flush folds the rare duplicates. Legal only while nothing resolves
  // against the live map (no same-segment lookup) and only for Terms-layout,
  // norm-free fields.
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
    uint32_t ids[2] = {std::numeric_limits<uint32_t>::max(),
                       std::numeric_limits<uint32_t>::max()};
    return VisitLog([&](auto& log) {
      return analysis::ForEachValidRow(
        fmt, count, [&](uint32_t r, uint32_t idx) {
          const bool value = data[idx];
          auto& id = ids[value];
          if (id == std::numeric_limits<uint32_t>::max()) {
            id = _dict.Insert(terms[value]);
          }
          return PushKeyword(log, first_doc + r, terms[value].GetSize(), id);
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

  // Caller contract: at least one selected row is invalid -- the null term
  // is interned eagerly.
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

  void ReserveTerms(size_t expected_terms) { _dict.Reserve(expected_terms); }

  void MarkUniqueTerms() {
    SDB_ASSERT(Layout() == TokenLayout::Terms);
    SDB_ASSERT(!_col_writer);
    SDB_ASSERT(!_dict.Size() || _unique_terms);
    _unique_terms = true;
    _dict.ShrinkEmptyMap();
  }

  void AppendInlineDoc(uint32_t id, doc_id_t doc) {
    SDB_ASSERT(id == _inline_docs.size());
    _inline_docs.push_back(doc);
  }

  // 1-1 fast path: every run is one doc's single token (a repeated doc is a
  // multi-value continuation); per-column hint set at bind, run/count
  // equality re-checked at the call site. The equality check cannot tell a
  // {0,2}-token run pair from all-ones: only kernels that are single-token
  // by construction may set the hint.
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

  // One doc's token run [base, base+n) of a batch, term ids already resolved
  // (`ids` pre-offset to the run). Semantics identical to Invert over the
  // same token sequence; the Terms and positional arms share nothing beyond
  // the per-doc reset, so each gets its own body.
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

  // Terms defers BeginDoc to an actual log push.
  template<typename Log>
  IRS_FORCE_INLINE bool PushDocTerms(Log& log, doc_id_t id, const uint32_t* ids,
                                     uint32_t n) {
    static_assert(Log::kLayout == TokenLayout::Terms);
    Reset<TokenLayout::Terms>(id);
    if (!n) [[unlikely]] {
      return true;
    }
    // Flush derives cursors from the log itself, so every reject must
    // happen before anything is pushed below.
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

  // Kernels emit value-absolute pos/offs across resumptions, so the
  // field-level bases are captured at value start and every batch of the
  // value rebases by them -- advancing the running base per batch would
  // double-shift continuation batches. Validation is read-only and precedes
  // the commit, which cannot fail: flush derives cursors from the log
  // itself, so every reject must happen before anything is pushed.
  template<typename Log>
  bool PushDocRun(Log& log, doc_id_t id, TokenBatch& batch, uint32_t base,
                  uint32_t n, const uint32_t* ids, bool value_start) {
    Reset<Log::kLayout>(id);

    if (!n) [[unlikely]] {
      log.TouchDoc(id);
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

  // Dense positions (inc==1 for every token) continue the running
  // position; the log materializes the ramp itself if the doc was already
  // promoted to explicit positions. A genuine (kernel-produced) non-dense
  // batch carries value-absolute positions and rebases by the value start.
  IRS_FORCE_INLINE std::optional<PosChecks> ValidatePos(const TokenBatch& batch,
                                                        uint32_t base,
                                                        uint32_t count) const {
    const auto* pos_arr = batch.pos + base;
    const bool dense = _dense_pos;
    const uint32_t pos_base = dense ? _state.last_pos : _state.value_pos;
    uint32_t last_pos;
    if (dense) {
      // All increments are 1: monotonic by construction, one eof check
      // covers the batch.
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

  // Offsets are value-absolute; rebase by the value's start base.
  // Validate in 64-bit and reject uint32 wraparound of the largest
  // rebased offset -- parity with the legacy per-token absolute check.
  // Ends are not required to be monotone, so the wrap check must cover
  // the maximum end, not the last one.
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

  // Parity with WriteOffset: Offs materializes into the field meta only once
  // offsets are actually indexed; the offset cursors advance in lockstep.
  IRS_FORCE_INLINE void AdvanceOffs(uint32_t start, uint32_t end) {
    _meta.index_features |= IndexFeatures::Offs;
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

  // Resolve every term of the batch to its id. Nothing is captured here:
  // the log moves only after a run's validation.
  const uint32_t* ResolveTerms(const duckdb::string_t* terms, size_t n) {
    _dict.Insert(std::span{terms, n}, std::span{_resolve->hashes},
                 std::span{_resolve->ids});
    return _resolve->ids;
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

  // Per-doc keyword push: the keyword block entries' per-value tail, with
  // the term id already resolved. Every reject precedes the capture/push
  // step.
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

  // Per-doc state reset; the layout parameter skips the position/offset
  // cursors a field of that layout never reads (defaults to the full reset
  // for layout-generic callers).
  template<TokenLayout kLayout = TokenLayout::TermsPosOffs>
  void Reset(doc_id_t doc_id) {
    if (doc_id == _state.last_doc) {
      return;
    }
    FinalizeNorm();
    _state.ResetDoc<kLayout>(doc_id);
  }

  // The previous doc's norm, emitted at doc transition and at flush.
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

  // Doc-scoped UNCOMMITTED state: everything a reject abandons while the log
  // (the committed record) stays untouched. Kernels emit value-absolute
  // pos/offs across resumptions; the value_* bases are captured at each
  // value's first batch so multi-batch continuations rebase by the value
  // start -- advancing a running base per batch would double-shift them.
  struct DocState {
    doc_id_t last_doc{doc_limits::invalid()};
    FieldStats stats;
    // Last committed absolute position within the current doc: the
    // monotonicity/eof checks and the dense running base rebase from it.
    uint32_t last_pos{0};
    // Running end-offset concatenation base / last written start offset
    // (start monotonicity + wraparound checks).
    uint32_t offs{0};
    uint32_t last_start_offs{0};
    // Doc-level bases captured at value start.
    uint32_t value_pos{0};
    uint32_t value_offs{0};
    // A value's batches are still streaming (tail-open batch seen).
    bool value_open{false};

    // Skips the cursors a field of `kLayout` never reads.
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

    // Leaves the state reading as "one keyword token pushed for last_doc" so
    // a same-doc continuation through a per-value entry resumes consistently
    // after a no-Reset bulk capture. The len under-report is legal only
    // while that path stays norm-free (no _col_writer consumes stats) and
    // Terms-layout (pos is never stored).
    void AssumeKeywordTail() noexcept {
      last_pos = 1;
      stats.len = 1;
    }
  };

  ResolveScratch* _resolve;
  TermDictionary _dict;
  ManagedVector<doc_id_t> _inline_docs;
  PostingLogVariant _log;
  ColWriter* _col_writer = nullptr;
  NormColumnWriter* _norm_writer = nullptr;
  uint32_t _norm_row_group_size = 0;
  FieldMeta _meta;
  IndexFeatures _requested_features{};
  bool _one_to_one = false;
  bool _unique_terms = false;
  bool _dense_pos = true;
  DocState _state;
};

class FieldsInverter : util::Noncopyable {
 public:
  explicit FieldsInverter(InverterMemory mem);
  ~FieldsInverter();

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

    FieldInverter::NormSpec norms;
    if (_col_writer && IsSubsetOf(IndexFeatures::Norm, index_features)) {
      SDB_ASSERT(_field_options,
                 "Norm-featured field requires per-field index options");
      norms = {.col_writer = _col_writer,
               .norm_id = _field_options->GetNormColumnId(id),
               .row_group_size = _field_options->row_group_size};
      SDB_ASSERT(field_limits::valid(norms.norm_id),
                 "GetNormColumnId must return a valid id for field ", id);
    }
    auto& field = _fields.emplace_back(id, _arena, _resolve, _mem.rm,
                                       index_features, norms);
    // Pre-size to the field's previous-segment term count: skips the rehash
    // chain on shape-similar bulk segments, self-corrects one segment after a
    // shape change. A row-count hint would over-reserve (sparse table = slower
    // probes); history never exceeds observed reality.
    slot.field = &field;
    if (slot.last_terms) {
      field.ReserveTerms(slot.last_terms);
    }
    return slot.field;
  }

  // Pre-seeds a field's reserve hint for its FIRST segment; afterwards the
  // slot self-corrects from observed reality (Reset), which always wins over
  // an estimate -- only an empty slot is filled. Estimates must be
  // ~exact-or-under (a raw row count over-reserves; sparse tables probe
  // slower and FlushRequired counts dictionary capacity).
  void SeedHistory(field_id id, uint32_t expected_terms) {
    auto& slot = _fields_map.try_emplace(id).first->second;
    if (!slot.last_terms) {
      slot.last_terms = expected_terms;
    }
  }

  void FinalizeNorms() {
    for (auto& field : _fields) {
      field.FinalizeNorm();
    }
  }

  // Defined in fields_inverter.cpp -- keeps burst_trie out of this header.
  void Flush(burst_trie::FieldWriter& fw, FlushState& state,
             std::span<const BasicTermReader* const> extra);

  size_t MemoryActive() const noexcept {
    return _arena.SizeInBytes() + FieldsMemory();
  }

  size_t MemoryReserved() const noexcept {
    return _arena.AllocationSize() + FieldsMemory();
  }

  // Slots outlive their fields: the pointer dies with every Reset, the last
  // observed term count stays behind as the next segment's reserve hint.
  void Reset() noexcept {
    for (auto& [id, slot] : _fields_map) {
      if (slot.field) {
        if (const auto n = slot.field->Dictionary().Size()) {
          slot.last_terms = static_cast<uint32_t>(n);
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
  };
  using FieldsMap = sdb::containers::FlatHashMap<field_id, FieldSlot>;

  // Retained history slots (null field) are writer-lifetime bookkeeping, not
  // segment data: only live fields count, so an empty writer reports zero and
  // history never inflates flush triggers. Log SoA columns live in the shared
  // arena (counted by the callers); add only the dictionary and the
  // per-doc/per-run bookkeeping vectors.
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
  ResolveScratch _resolve;
  std::deque<FieldInverter, ManagedTypedAllocator<FieldInverter>> _fields;
  FieldsMap _fields_map;
  std::unique_ptr<ScatterScratch> _scatter;
  ColWriter* _col_writer = nullptr;
  const IndexFieldOptions* _field_options = nullptr;
};

}  // namespace irs
