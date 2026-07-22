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

#include <deque>
#include <span>

#include "basics/containers/flat_hash_map.h"
#include "basics/log.h"
#include "basics/noncopyable.hpp"
#include "iresearch/analysis/batch/token_batch.hpp"
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
//   verbatim keyword / PK blocks  -> InvertKeywordBlock (docs span / ramp)
//   constant term (null, bool)    -> InvertConstantBlock
//   analyzed columns (runs)       -> InvertBlock(batch, runs)
//   numeric trie slabs (strided)  -> InvertBlock(terms, docs, tokens_per_doc)
// Everything else in this class is machinery below those four.
class FieldInverter : util::Noncopyable {
 public:
  FieldInverter(field_id id, duckdb::ArenaAllocator& arena,
                ResolveScratch& resolve, IResourceManager& rm,
                IndexFeatures index_features, ColWriter* col_writer = nullptr,
                NormColumnOptions norm_options = {})
    : _resolve{&resolve},
      _dict{arena, rm},
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

  const FieldMeta& Meta() const noexcept { return _meta; }
  const FieldStats& Stats() const noexcept { return _state.stats; }
  IndexFeatures RequestedFeatures() const noexcept {
    return _requested_features;
  }
  const TermDictionary& Dictionary() const noexcept { return _dict; }
  TermDictionary& Dictionary() noexcept { return _dict; }
  void ReserveTerms(size_t expected_terms) { _dict.Reserve(expected_terms); }
  void MarkUniqueTerms() noexcept {
    SDB_ASSERT(Layout() == TokenLayout::Terms);
    SDB_ASSERT(!_col_writer);
    _unique_terms = true;
  }
  bool UniqueTerms() const noexcept { return _unique_terms; }
  void SetUnique(bool value) noexcept { _unique = value; }
  void SetDensePos(bool value) noexcept { _dense_pos = value; }
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

  void ComputeFeatures() const {
    SDB_ASSERT(_col_writer);
    if (!_norm_writer) {
      _norm_writer =
        &_col_writer->OpenNormColumn(_meta.norm, _norm_row_group_size);
    }
    const auto target_row =
      static_cast<uint64_t>(_state.last_doc) - doc_limits::min();
    _norm_writer->Append(target_row, _state.stats.len);
  }

  bool InvertKeywordBlock(std::span<const duckdb::string_t> values,
                          std::span<const doc_id_t> docs) {
    SDB_ASSERT(values.size() == docs.size());
    return InvertKeywordImpl(values,
                             [docs](size_t k) noexcept { return docs[k]; });
  }

  template<typename T>
  bool InvertKeywordBlock(std::span<const T> values, doc_id_t first_doc) {
    return InvertKeywordImpl(values, [first_doc](size_t k) noexcept {
      return first_doc + static_cast<doc_id_t>(k);
    });
  }

  bool InvertConstantBlock(bytes_view term, std::span<const doc_id_t> docs) {
    const auto id = _dict.Resolve(term);
    return VisitLog([&](auto& log) {
      for (size_t j = 0; j < docs.size(); ++j) {
        if (!PushKeyword(log, docs[j], term, id)) [[unlikely]] {
          return false;
        }
      }
      return true;
    });
  }

  // Terms-layout strided ingest straight from a terms array -- no TokenBatch:
  // tokens [k*tokens_per_doc, (k+1)*tokens_per_doc) belong to docs[k].
  bool InvertBlock(std::span<const duckdb::string_t> terms,
                   std::span<const doc_id_t> docs, uint32_t tokens_per_doc) {
    SDB_ASSERT(tokens_per_doc != 0);
    SDB_ASSERT(terms.size() == docs.size() * tokens_per_doc);
    SDB_ASSERT(terms.size() <= TokenBatch::kCapacity);
    SDB_ASSERT(!_state.value_open);
    ResolveTerms(terms.data(), terms.size());
    return VisitLog([&](auto& log) {
      using LogT = std::remove_cvref_t<decltype(log)>;
      if constexpr (LogT::kLayout != TokenLayout::Terms) {
        SDB_ASSERT(false);
        return false;
      } else {
        auto* const ids = _resolve->ids;
        uint32_t tok = 0;
        for (const auto doc : docs) {
          if (!PushDocTerms(log, doc, ids + tok, tokens_per_doc)) [[unlikely]] {
            return false;
          }
          tok += tokens_per_doc;
        }
        return true;
      }
    });
  }

  bool InvertBlock(TokenBatch& batch, std::span<const DocRun> runs) {
    bool last_cut = false;
    if (!runs.empty() && runs.back().doc == DocRun::kOpenValue) {
      last_cut = true;
      runs = runs.subspan(0, runs.size() - 1);
    }
    return VisitLog([&](auto& log) {
      auto* ids = ResolveBlock(batch);

      const bool first_continues = _state.value_open;
      if (_unique && !first_continues && !last_cut &&
          runs.size() == batch.count) {
        return InvertOneToOne(log, batch, runs, ids);
      }
      uint32_t tok = 0;
      for (size_t r = 0; r < runs.size(); ++r) {
        const auto& run = runs[r];
        if (!TokenRun(log, run.doc, batch, tok, run.ntokens, ids + tok,
                      r == 0 ? !first_continues : true)) [[unlikely]] {
          return false;
        }
        tok += run.ntokens;
      }
      if (!runs.empty() || last_cut) {
        _state.value_open = last_cut;
      }
      SDB_ASSERT(tok == batch.count);
      return true;
    });
  }

 private:
  friend class FieldsInverter;

  static constexpr uint32_t kDynTokens = 0;

  // 1-1 fast path: every run is one distinct doc's single token (per-column
  // hint set at bind, run/count equality re-checked at the call site);
  // TokenRun<1> constant-folds the per-run scans.
  template<typename Log>
  IRS_NO_INLINE bool InvertOneToOne(Log& log, TokenBatch& batch,
                                    std::span<const DocRun> runs,
                                    uint32_t* ids) {
    for (uint32_t i = 0, n = batch.count; i < n; ++i) {
      SDB_ASSERT(runs[i].ntokens == 1);
      if (!TokenRun<1>(log, runs[i].doc, batch, i, 1, ids + i, true))
        [[unlikely]] {
        return false;
      }
    }
    return true;
  }

  // One doc's token run [base, base+n) of a batch, term ids already resolved
  // (`ids` pre-offset to the run). Semantics identical to Invert over the
  // same token sequence; the Terms and positional arms share nothing beyond
  // the per-doc reset, so each gets its own body. kN pins the token count at
  // compile time (kDynTokens = runtime n).
  template<uint32_t kN = kDynTokens, typename Log>
  IRS_FORCE_INLINE bool TokenRun(Log& log, doc_id_t id, TokenBatch& batch,
                                 uint32_t base, uint32_t n, uint32_t* ids,
                                 bool value_start) {
    SDB_ASSERT(id < doc_limits::eof());
    SDB_ASSERT(base + n <= batch.count);
    if constexpr (Log::kLayout == TokenLayout::Terms) {
      return PushDocTerms<Log, kN>(log, id, ids, n);
    } else {
      return PushDocRun<Log, kN>(log, id, batch, base, n, ids, value_start);
    }
  }

  // Terms defers BeginDoc to an actual log push (first occurrences are
  // captured inline in the dictionary and never reach the log).
  template<typename Log, uint32_t kN = kDynTokens>
  IRS_FORCE_INLINE bool PushDocTerms(Log& log, doc_id_t id, uint32_t* ids,
                                     uint32_t n) {
    static_assert(Log::kLayout == TokenLayout::Terms);
    Reset<TokenLayout::Terms>(id);
    const uint32_t count = kN == kDynTokens ? n : kN;
    if constexpr (kN == kDynTokens) {
      if (!count) [[unlikely]] {
        return true;
      }
      // Flush derives cursors from the log itself, so every reject must
      // happen before anything is pushed below.
      if (!CheckDocBudget(id, count)) [[unlikely]] {
        return false;
      }
    }
    if constexpr (kN == 1) {
      _state.stats.len += 1;
      if (!_dict.TryInline(ids[0], id)) {
        log.PushBatch(id, {ids, 1});
      }
      return true;
    } else {
      uint32_t kept = 0;
      for (uint32_t i = 0; i < count; ++i) {
        const auto term_id = ids[i];
        if (!_dict.TryInline(term_id, id)) {
          ids[kept++] = term_id;
        }
      }
      _state.stats.len += count;
      if (kept) {
        log.PushBatch(id, {ids, kept});
      }
      return true;
    }
  }

  struct PosChecks {
    uint32_t pos_base;
    uint32_t last_pos;
    uint32_t overlap;
    bool dense;
  };

  // Kernels emit value-absolute pos/offs across resumptions, so the
  // field-level bases are captured at value start and every batch of the
  // value rebases by them -- advancing the running base per batch would
  // double-shift continuation batches. Validation is read-only and precedes
  // the commit, which cannot fail: flush derives cursors from the log
  // itself, so every reject must happen before anything is pushed.
  template<typename Log, uint32_t kN = kDynTokens>
  bool PushDocRun(Log& log, doc_id_t id, TokenBatch& batch, uint32_t base,
                  uint32_t n, uint32_t* ids, bool value_start) {
    constexpr auto kLayout = Log::kLayout;
    Reset<kLayout>(id);

    const uint32_t count = kN == kDynTokens ? n : kN;
    if constexpr (kN == kDynTokens) {
      if (!count) [[unlikely]] {
        log.TouchDoc(id);
        return true;
      }
    }

    if (value_start) {
      _state.CaptureValueBases<kLayout>();
    }

    PosChecks checks;
    if (!ValidatePos<kN>(batch, base, count, checks)) [[unlikely]] {
      return false;
    }
    if constexpr (kLayout == TokenLayout::TermsPosOffs) {
      if (!ValidateOffs<kN>(batch, base, count)) [[unlikely]] {
        return false;
      }
    }
    if (!CheckDocBudget(id, count)) [[unlikely]] {
      return false;
    }

    CommitRun<Log, kN>(log, id, ids, batch, base, count, checks);
    return true;
  }

  // Dense positions (inc==1 for every token) continue the running
  // position; the log materializes the ramp itself if the doc was already
  // promoted to explicit positions. A genuine (kernel-produced) non-dense
  // batch carries value-absolute positions and rebases by the value start.
  template<uint32_t kN>
  IRS_FORCE_INLINE bool ValidatePos(const TokenBatch& batch, uint32_t base,
                                    uint32_t n, PosChecks& out) const {
    const uint32_t count = kN == kDynTokens ? n : kN;
    const auto* pos_arr = batch.pos + base;
    const bool dense = _dense_pos;
    const uint32_t pos_base = dense ? _state.pos : _state.value_pos;
    uint32_t overlap = 0;
    uint32_t last_pos;
    if (dense) {
      // All increments are 1: monotonic and overlap-free by construction,
      // one eof check covers the batch.
      last_pos = _state.pos + count;
      if (last_pos < _state.pos || last_pos >= pos_limits::eof()) [[unlikely]] {
        SDB_ERROR(IRESEARCH, "invalid position in field '", _meta.id, "'");
        return false;
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
        return false;
      }
    }
    out = {pos_base, last_pos, overlap, dense};
    return true;
  }

  // Offsets are value-absolute; rebase by the value's start base.
  // Validate in 64-bit and reject uint32 wraparound of the largest
  // rebased offset -- parity with the legacy per-token absolute check.
  // Ends are not required to be monotone, so the wrap check must cover
  // the maximum end, not the last one.
  template<uint32_t kN>
  IRS_FORCE_INLINE bool ValidateOffs(const TokenBatch& batch, uint32_t base,
                                     uint32_t n) const {
    const uint32_t count = kN == kDynTokens ? n : kN;
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

  template<typename Log, uint32_t kN>
  IRS_FORCE_INLINE void CommitRun(Log& log, doc_id_t id, uint32_t* ids,
                                  const TokenBatch& batch, uint32_t base,
                                  uint32_t n, const PosChecks& checks) {
    const uint32_t count = kN == kDynTokens ? n : kN;
    const auto* pos_arr = batch.pos + base;
    if constexpr (Log::kLayout == TokenLayout::TermsPos) {
      log.PushBatch(id, {ids, count}, checks.dense, {pos_arr, count},
                    checks.pos_base);
    } else {
      log.PushBatch(id, {ids, count}, checks.dense, {pos_arr, count},
                    checks.pos_base, {batch.offs_start + base, count},
                    {batch.offs_end + base, count}, _state.value_offs);
      // Parity with WriteOffset: Offs materializes into the field meta only
      // once offsets are actually indexed.
      _meta.index_features |= IndexFeatures::Offs;
      _state.last_start_offs =
        _state.value_offs + batch.offs_start[base + count - 1];
      _state.offs = _state.value_offs + batch.offs_end[base + count - 1];
    }
    _state.stats.len += count;
    _state.stats.num_overlap += checks.overlap;
    _state.pos = checks.last_pos;
    _state.last_pos = checks.last_pos;
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

  static bytes_view ToBytes(const duckdb::string_t& s) noexcept {
    return {reinterpret_cast<const byte_type*>(s.GetData()), s.GetSize()};
  }

  static bytes_view ToBytes(std::string_view s) noexcept {
    return {reinterpret_cast<const byte_type*>(s.data()), s.size()};
  }

  // Dictionaries at or below this many distinct terms stay L2-resident, so the
  // probe hits cache and the prefetch-pipelined AddBatch can't pay back its
  // extra hash/id passes; above it the table goes cold and prefetch wins.
  static constexpr size_t kFusedProbeThreshold = 1024;

  // Resolve every term of the batch to its id. Id-mode batches arrive
  // pre-resolved at emit; byte mode picks fused vs prefetch-pipelined by
  // dictionary size (a hot table can't pay back the extra hash pass, a cold
  // one hides its misses behind the prefetch runway). The returned pointer is
  // writable scratch -- the Terms capture path compacts ids in place. Nothing
  // is captured here: slots and log move only after a run's validation.
  uint32_t* ResolveBlock(TokenBatch& batch) {
    // The scratch arrays (and every batch consumer) are sized to kCapacity.
    SDB_ASSERT(batch.count <= TokenBatch::kCapacity);
    ResolveTerms(batch.terms, batch.count);
    return _resolve->ids;
  }

  template<typename T>
  void ResolveTerms(const T* terms, size_t n) {
    auto* const ids = _resolve->ids;
    if (_dict.Size() < kFusedProbeThreshold) {
      for (size_t j = 0; j < n; ++j) {
        ids[j] = _dict.Resolve(terms[j], TermDictionary::TermHash(terms[j]));
      }
    } else {
      auto* const hashes = _resolve->hashes;
      for (size_t j = 0; j < n; ++j) {
        hashes[j] = TermDictionary::TermHash(terms[j]);
      }
      _dict.ResolveBatch(std::span<const T>{terms, n}, {hashes, n}, ids);
    }
  }

  template<typename T, typename DocAt>
  bool InvertKeywordImpl(std::span<const T> values, DocAt doc_at) {
    if (_unique_terms) {
      for (size_t k = 0; k < values.size(); ++k) {
        SDB_ASSERT(doc_at(k) < doc_limits::eof());
        SDB_ASSERT(doc_at(k) >= _state.last_doc);
        _dict.Append(values[k], doc_at(k));
      }
      if (!values.empty()) {
        Reset(doc_at(values.size() - 1));
        _state.pos = 1;
        _state.last_pos = 1;
        _state.stats.len = 1;
      }
      return true;
    }
    return VisitLog([&](auto& log) {
      auto* const ids = _resolve->ids;
      for (size_t base = 0; base < values.size();
           base += TokenBatch::kCapacity) {
        const auto n = std::min(values.size() - base, TokenBatch::kCapacity);
        ResolveTerms(values.data() + base, n);
        if constexpr (std::decay_t<decltype(log)>::kLayout ==
                      TokenLayout::Terms) {
          if (!_col_writer) {
            CaptureKeywordTerms(log, ids, doc_at, base, n);
            continue;
          }
        }
        for (size_t j = 0; j < n; ++j) {
          const auto k = base + j;
          if (!PushKeyword(log, doc_at(k), ToBytes(values[k]), ids[j]))
            [[unlikely]] {
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
    for (size_t j = 0; j < n; ++j) {
      const auto doc = doc_at(base + j);
      SDB_ASSERT(doc < doc_limits::eof());
      SDB_ASSERT(doc >= _state.last_doc);
      if (!_dict.TryInline(ids[j], doc)) {
        log.Push(doc, ids[j]);
      }
    }
    Reset(doc_at(base + n - 1));
    _state.pos = 1;
    _state.last_pos = 1;
    _state.stats.len = 1;
  }

  // Per-doc keyword push: the keyword block entries' per-value tail, with
  // the term id already resolved. Every reject precedes the capture/push
  // step.
  template<typename Log>
  bool PushKeyword(Log& log, doc_id_t id, bytes_view value, uint32_t term_id) {
    constexpr auto kLayout = Log::kLayout;
    SDB_ASSERT(id < doc_limits::eof());

    Reset(id);

    ++_state.pos;
    if (_state.pos >= pos_limits::eof()) [[unlikely]] {
      SDB_ERROR(IRESEARCH, "invalid position ", _state.pos,
                " >= ", pos_limits::eof(), " in field '", _meta.id, "'");
      return false;
    }
    if (!CheckDocBudget(id, 1)) [[unlikely]] {
      return false;
    }

    // A Terms doc whose tokens are all first occurrences (PK-shaped columns)
    // never touches the log at all.
    if constexpr (kLayout == TokenLayout::Terms) {
      if (!_dict.TryInline(term_id, id)) {
        log.Push(id, term_id);
      }
    } else if constexpr (kLayout == TokenLayout::TermsPos) {
      log.PushOne(id, term_id, _state.pos);
    } else {
      const auto size = static_cast<uint32_t>(value.size());
      log.PushOne(id, term_id, _state.pos, _state.offs, _state.offs + size);
      _state.last_start_offs = _state.offs;
      _meta.index_features |= IndexFeatures::Offs;
    }

    ++_state.stats.len;
    _state.last_pos = _state.pos;
    _state.offs += static_cast<uint32_t>(value.size());
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
    if (_col_writer && doc_limits::valid(_state.last_doc)) {
      ComputeFeatures();
    }
    _state.ResetDoc<kLayout>(doc_id);
  }

  void FinalizeNorm() const {
    if (_col_writer && doc_limits::valid(_state.last_doc)) {
      ComputeFeatures();
    }
  }

  // Doc-scoped UNCOMMITTED state: everything a reject abandons while the log
  // (the committed record) stays untouched. Kernels emit value-absolute
  // pos/offs across resumptions; the value_* bases are captured at each
  // value's first batch so multi-batch continuations rebase by the value
  // start -- advancing a running base per batch would double-shift them.
  struct DocState {
    doc_id_t last_doc{doc_limits::invalid()};
    FieldStats stats;
    // Running / last-written absolute position within the current doc
    // (last_pos trails pos for first-token overlap detection).
    uint32_t pos{pos_limits::invalid()};
    uint32_t last_pos{0};
    // Running end-offset concatenation base / last written start offset
    // (start monotonicity + wraparound checks).
    uint32_t offs{0};
    uint32_t last_start_offs{0};
    // Doc-level bases captured at value start.
    uint32_t value_pos{0};
    uint32_t value_offs{0};
    // A value's batches are still streaming (kOpenValue continuation seen).
    bool value_open{false};

    // Skips the cursors a field of `kLayout` never reads.
    template<TokenLayout kLayout>
    void ResetDoc(doc_id_t doc) noexcept {
      stats = {};
      last_doc = doc;
      if constexpr (kLayout != TokenLayout::Terms) {
        pos = pos_limits::invalid();
        last_pos = 0;
      }
      if constexpr (kLayout == TokenLayout::TermsPosOffs) {
        offs = 0;
        last_start_offs = 0;
      }
    }

    template<TokenLayout kLayout>
    void CaptureValueBases() noexcept {
      value_pos = pos;
      if constexpr (kLayout == TokenLayout::TermsPosOffs) {
        value_offs = offs;
      }
    }
  };

  ResolveScratch* _resolve;
  TermDictionary _dict;
  PostingLogVariant _log;
  ColWriter* _col_writer = nullptr;
  mutable NormColumnWriter* _norm_writer = nullptr;
  uint32_t _norm_row_group_size = 0;
  FieldMeta _meta;
  IndexFeatures _requested_features{};
  bool _unique_terms = false;
  bool _unique = false;
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

  void FinalizeNorms() const {
    for (const auto& field : _fields) {
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
      size += field._dict.Memory() + field.VisitLog([](const auto& l) {
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
