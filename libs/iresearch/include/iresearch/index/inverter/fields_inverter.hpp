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
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/inverter/posting_log.hpp"
#include "iresearch/index/inverter/term_dictionary.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct FlushState;
struct BasicTermReader;
struct ScatterScratch;

namespace burst_trie {

class FieldWriter;
}

// Byte-mode term-resolution scratch (hashes + resolved ids for one batch),
// owned by the writer and shared by all its fields: id-mode batches never
// touch it, so it has no place on the per-call stack.
struct ResolveScratch {
  uint64_t hashes[TokenBatch::kCapacity];
  uint32_t ids[TokenBatch::kCapacity];
};

// One inverter per field. Ingest entries, by column class (level-1 routing):
//   verbatim keyword blocks       -> InvertKeywordBlock (docs span)
//                                    / InvertDenseKeywordBlock (doc ramp)
//   PK blocks (unique terms)      -> InvertUniqueKeywordBlock (ramp)
//   constant term (null, bool)    -> InvertConstantBlock
//   analyzed columns (runs)       -> InvertBlock(batch, runs)
//   numeric trie slabs (strided)  -> InvertStridedBlock(terms, docs,
//   tokens_per_doc)
// Everything else in this class is machinery below those five.
class FieldInverter : util::Noncopyable {
 public:
  FieldInverter(field_id id, duckdb::ArenaAllocator& arena,
                ResolveScratch& resolve, IResourceManager& rm,
                IndexFeatures index_features, ColWriter* col_writer = nullptr,
                NormColumnOptions norm_options = {})
    : _resolve{&resolve},
      _dict{arena, rm},
      _inline_docs{ManagedTypedAllocator<doc_id_t>{rm}},
      _log{MakePostingLog(arena, rm, LayoutFromFeatures(index_features))},
      _meta{id, index_features & (~IndexFeatures::Offs)},
      _requested_features{index_features} {
    if (IsSubsetOf(IndexFeatures::Norm, index_features) && col_writer &&
        field_limits::valid(norm_options.id)) {
      _col_writer = col_writer;
      _norm_row_group_size = norm_options.row_group_size;
      _meta.norm = norm_options.id;
    }
  }

  const auto& Meta() const noexcept { return _meta; }
  const auto& Stats() const noexcept { return _state.stats; }
  IndexFeatures RequestedFeatures() const noexcept {
    return _requested_features;
  }
  auto& Dictionary(this auto& self) noexcept { return self._dict; }
  std::span<const doc_id_t> InlineDocs() const noexcept {
    return {_inline_docs.data(), _inline_docs.size()};
  }

  void ReserveTerms(size_t expected_terms) { _dict.Reserve(expected_terms); }
  void SetOneToOne(bool value) noexcept { _one_to_one = value; }
  void SetDensePos(bool value) noexcept { _dense_pos = value; }
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

  bool InvertKeywordBlock(std::span<const duckdb::string_t> values,
                          std::span<const doc_id_t> docs) {
    SDB_ASSERT(values.size() == docs.size());
    return InvertKeywordImpl(values,
                             [docs](size_t k) noexcept { return docs[k]; });
  }

  bool InvertDenseKeywordBlock(std::span<const duckdb::string_t> values,
                               doc_id_t first_doc) {
    return InvertKeywordImpl(values, [first_doc](size_t k) noexcept {
      return first_doc + static_cast<doc_id_t>(k);
    });
  }

  // The PK shape: terms are one-per-doc and (almost always) distinct, so
  // interning is append-only -- keyword blocks skip the hash/probe entirely
  // and flush folds the rare duplicates. Legal only while nothing resolves
  // against the live map (no same-segment lookup) and only for Terms-layout,
  // norm-free fields.
  bool InvertUniqueKeywordBlock(std::span<const duckdb::string_t> values,
                                doc_id_t first_doc) {
    MarkUniqueTerms();
    return InvertDenseKeywordBlock(values, first_doc);
  }

  bool InvertConstantBlock(bytes_view term, std::span<const doc_id_t> docs) {
    const auto id = _dict.Insert(MakeTermView(term));
    const auto size = static_cast<uint32_t>(term.size());
    return VisitLog([&](auto& log) {
      for (size_t j = 0; j < docs.size(); ++j) {
        if (!PushKeyword(log, docs[j], size, id)) [[unlikely]] {
          return false;
        }
      }
      return true;
    });
  }

  // Terms-layout strided ingest straight from a terms array -- no TokenBatch:
  // tokens [k*tokens_per_doc, (k+1)*tokens_per_doc) belong to docs[k].
  bool InvertStridedBlock(std::span<const duckdb::string_t> terms,
                          std::span<const doc_id_t> docs,
                          uint32_t tokens_per_doc) {
    SDB_ASSERT(tokens_per_doc != 0);
    SDB_ASSERT(terms.size() == docs.size() * tokens_per_doc);
    SDB_ASSERT(!_state.value_open);
    SDB_ASSERT(Layout() == TokenLayout::Terms);
    auto& log = *std::get_if<PostingLog<TokenLayout::Terms>>(&_log);
    auto* const ids = ResolveTerms(terms.data(), terms.size());
    uint32_t tok = 0;
    for (const auto doc : docs) {
      if (!PushDocTerms(log, doc, ids + tok, tokens_per_doc)) [[unlikely]] {
        return false;
      }
      tok += tokens_per_doc;
    }
    return true;
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
  IRS_NO_INLINE bool InvertOneToOne(Log& log, TokenBatch& batch,
                                    DocRuns runs,
                                    uint32_t* ids) {
    for (uint32_t i = 0, n = batch.count; i < n; ++i) {
      SDB_ASSERT(runs[i].ntokens == 1);
      SDB_ASSERT(i == 0 || runs[i].doc >= runs[i - 1].doc);
      if (!PushDoc(log, runs[i].doc, batch, i, 1, ids + i, true))
        [[unlikely]] {
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
                                uint32_t base, uint32_t n, uint32_t* ids,
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
    uint32_t overlap;
  };

  // Kernels emit value-absolute pos/offs across resumptions, so the
  // field-level bases are captured at value start and every batch of the
  // value rebases by them -- advancing the running base per batch would
  // double-shift continuation batches. Validation is read-only and precedes
  // the commit, which cannot fail: flush derives cursors from the log
  // itself, so every reject must happen before anything is pushed.
  template<typename Log>
  bool PushDocRun(Log& log, doc_id_t id, TokenBatch& batch, uint32_t base,
                  uint32_t n, uint32_t* ids, bool value_start) {
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
  IRS_FORCE_INLINE std::optional<PosChecks> ValidatePos(
    const TokenBatch& batch, uint32_t base, uint32_t count) const {
    const auto* pos_arr = batch.pos + base;
    const bool dense = _dense_pos;
    const uint32_t pos_base = dense ? _state.last_pos : _state.value_pos;
    uint32_t overlap = 0;
    uint32_t last_pos;
    if (dense) {
      // All increments are 1: monotonic and overlap-free by construction,
      // one eof check covers the batch.
      last_pos = _state.last_pos + count;
      if (last_pos < _state.last_pos || last_pos >= pos_limits::eof())
        [[unlikely]] {
        SDB_ERROR(IRESEARCH, "invalid position in field '", _meta.id, "'");
        return std::nullopt;
      }
    } else {
      // First-token overlap: increment 0 leaves the absolute position equal
      // to the previous token's, uniformly across value starts, multi-value
      // and multi-batch continuations.
      overlap = (pos_base + pos_arr[0] == _state.last_pos);
      bool monotonic = pos_base + pos_arr[0] >= _state.last_pos;
      for (uint32_t i = 1; i < count; ++i) {
        monotonic &= pos_arr[i] >= pos_arr[i - 1];
        overlap += (pos_arr[i] == pos_arr[i - 1]);
      }
      last_pos = pos_base + pos_arr[count - 1];
      if (!monotonic || last_pos < pos_base || last_pos >= pos_limits::eof())
        [[unlikely]] {
        SDB_ERROR(IRESEARCH, "invalid position in field '", _meta.id, "'");
        return std::nullopt;
      }
    }
    return PosChecks{pos_base, last_pos, overlap};
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
  IRS_FORCE_INLINE void CommitRun(Log& log, doc_id_t id, uint32_t* ids,
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
    _state.AdvancePos(checks.last_pos, checks.overlap, count);
  }

  IRS_FORCE_INLINE bool CheckDocBudget(doc_id_t doc, uint64_t n) const {
    if (_state.stats.len + n > std::numeric_limits<uint32_t>::max())
      [[unlikely]] {
      SDB_ERROR(IRESEARCH, "too many tokens in field: ", _meta.id,
                ", document: ", doc);
      return false;
    }
    return true;
  }

  // Resolve every term of the batch to its id. Id-mode batches arrive
  // pre-resolved at emit. The returned pointer is writable scratch -- the
  // Terms capture path compacts ids in place. Nothing is captured here:
  // slots and log move only after a run's validation.
  uint32_t* ResolveTerms(const duckdb::string_t* terms, size_t n) {
    _dict.Insert(std::span{terms, n}, std::span{_resolve->hashes},
                 std::span{_resolve->ids});
    return _resolve->ids;
  }

  template<typename DocAt>
  IRS_FORCE_INLINE void InvertUniqueKeywords(
    std::span<const duckdb::string_t> values, DocAt doc_at) {
    SDB_ASSERT(Layout() == TokenLayout::Terms);
    SDB_ASSERT(!_col_writer);
    for (size_t k = 0; k < values.size(); ++k) {
      SDB_ASSERT(doc_at(k) < doc_limits::eof());
      SDB_ASSERT(doc_at(k) >= _state.last_doc);
      AppendInlineDoc(_dict.AppendUnique(values[k]), doc_at(k));
    }
    if (!values.empty()) {
      Reset(doc_at(values.size() - 1));
      _state.AssumeKeywordTail();
    }
  }

  template<typename DocAt>
  bool InvertKeywordImpl(std::span<const duckdb::string_t> values,
                         DocAt doc_at) {
    if (_unique_terms) {
      InvertUniqueKeywords(values, doc_at);
      return true;
    }
    return VisitLog([&]<typename LogT>(LogT& log) {
      for (size_t base = 0; base < values.size();
           base += TokenBatch::kCapacity) {
        const auto n = std::min(values.size() - base, TokenBatch::kCapacity);
        auto* const ids = ResolveTerms(values.data() + base, n);
        if constexpr (LogT::kLayout == TokenLayout::Terms) {
          if (!_col_writer) {
            CaptureKeywordTerms(log, ids, doc_at, base, n);
            continue;
          }
        }
        for (size_t j = 0; j < n; ++j) {
          const auto k = base + j;
          if (!PushKeyword(log, doc_at(k),
                           values[k].GetSize(),
                           ids[j])) [[unlikely]] {
            return false;
          }
        }
      }
      return true;
    });
  }

  template<typename Log, typename DocAt>
  void CaptureKeywordTerms(Log& log, const uint32_t* ids, DocAt doc_at,
                           size_t base, size_t n) {
    static_assert(Log::kLayout == TokenLayout::Terms);
    SDB_ASSERT(!_col_writer);
    for (size_t j = 0; j < n; ++j) {
      const auto doc = doc_at(base + j);
      SDB_ASSERT(doc < doc_limits::eof());
      SDB_ASSERT(doc >= _state.last_doc);
      log.Push(doc, ids[j]);
    }
    Reset(doc_at(base + n - 1));
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
      log.Push(id, term_id);
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
      _state.AdvancePos(pos, 0, 1);
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
    // Last committed absolute position within the current doc: overlap
    // detection, monotonicity/eof checks and the dense running base all
    // rebase from it.
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

    void AdvancePos(uint32_t last, uint32_t overlap, uint32_t count) noexcept {
      stats.len += count;
      stats.num_overlap += overlap;
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

    NormColumnOptions norm_options{};
    if (_col_writer && IsSubsetOf(IndexFeatures::Norm, index_features)) {
      SDB_ASSERT(_field_options,
                 "Norm-featured field requires per-field index options");
      norm_options = _field_options->GetNormColumnOptions(id);
      SDB_ASSERT(field_limits::valid(norm_options.id),
                 "GetNormColumnOptions must return a valid id for field ", id);
    }
    auto& field = _fields.emplace_back(
      id, _arena, _resolve, _mem.rm, index_features, _col_writer, norm_options);
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
      size += field._dict.Memory() +
              field._inline_docs.capacity() *
                sizeof(decltype(field._inline_docs)::value_type) +
              field.VisitLog([](const auto& l) {
                return l.BookkeepingMemory();
              });
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
