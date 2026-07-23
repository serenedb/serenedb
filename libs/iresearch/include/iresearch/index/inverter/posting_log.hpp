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

#include <variant>

#include "basics/noncopyable.hpp"
#include "basics/system-compiler.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/inverter/inverter_memory.hpp"
#include "iresearch/types.hpp"

namespace irs {

using LogColumn = PackedU32Column;

constexpr TokenLayout LayoutFromFeatures(IndexFeatures features) noexcept {
  if (IndexFeatures::None == (features & IndexFeatures::Pos)) {
    return TokenLayout::Terms;
  }
  if (IndexFeatures::None == (features & IndexFeatures::Offs)) {
    return TokenLayout::TermsPos;
  }
  return TokenLayout::TermsPosOffs;
}

// Doc<->occurrence mapping for one field's log. Occurrences are stored in doc
// order with no doc id per occurrence; instead Run headers over the doc-id
// space plus a per-doc token count map occurrence positions back to docs.
// RunLog owns the doc-ordinal timeline: DocCount() is the number of docs
// recorded so far and the domain of every per-doc structure layered on top
// (the explicit-position bitmap in PostingLogPosBase). Runs and per-doc counts
// advance together in BeginDoc, so the two can never desync.
class RunLog {
 public:
  struct Run {
    doc_id_t first_doc;
    uint32_t ndocs;
  };

  RunLog(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : _runs{ManagedTypedAllocator<Run>{rm}}, _doc_tokens{blocks} {}

  // Opens `doc` as the current doc: extends the open run when consecutive,
  // bridges a short gap with zero-count doc slots (4B each -- cheaper than a
  // 16B run header, breakeven at 4), else starts a new run. A repeated doc
  // (multi-value continuation) is a no-op.
  void BeginDoc(doc_id_t doc) {
    if (!_runs.empty()) {
      auto& run = _runs.back();
      const auto last_doc = run.first_doc + run.ndocs - 1;
      if (doc == last_doc) {
        return;
      }
      SDB_ASSERT(doc > last_doc);
      const auto gap = doc - last_doc - 1;
      if (gap <= kMaxBridgedGap) {
        run.ndocs += gap + 1;
        for (doc_id_t k = 0; k <= gap; ++k) {
          _doc_tokens.Push(0);
        }
        return;
      }
    }
    _runs.push_back({doc, 1});
    _doc_tokens.Push(0);
  }

  // Adds `n` tokens to the current (open) doc.
  void AddTokens(uint32_t n) { _doc_tokens.IncBack(n); }

  // Number of docs recorded so far -- the ordinal domain shared by every
  // per-doc structure layered on the log.
  size_t DocCount() const noexcept { return _doc_tokens.Size(); }
  // Token count of the current (open) doc.
  uint32_t LastTokens() const noexcept { return _doc_tokens.Back(); }

  std::span<const Run> Runs() const noexcept {
    return {_runs.data(), _runs.size()};
  }
  const LogColumn& DocTokens() const noexcept { return _doc_tokens; }

  // Only the run headers are counted: _doc_tokens lives in the shared block
  // arena, accounted by FieldsInverter via arena size (avoids double count).
  size_t Memory() const noexcept { return _runs.capacity() * sizeof(Run); }

 private:
  static constexpr doc_id_t kMaxBridgedGap = 3;

  ManagedVector<Run> _runs;
  LogColumn _doc_tokens;
};

// Per-field occurrence log. The sink writes column-by-column (field-major per
// chunk, docs ascending within a field), so per-field ownership keeps token
// columns contiguous per field and lets runs extend across chunk boundaries;
// a run only breaks on a doc gap. PostingLog<L> owns exactly the columns
// its layout indexes.
class PostingLogBase : util::Noncopyable {
 public:
  void TouchDoc(doc_id_t doc) { _runlog.BeginDoc(doc); }

  uint64_t Size() const noexcept { return _term_ids.Size(); }

  std::span<const RunLog::Run> Runs() const noexcept { return _runlog.Runs(); }
  const LogColumn& DocTokens() const noexcept { return _runlog.DocTokens(); }
  const LogColumn& TermIds() const noexcept { return _term_ids; }

  size_t BookkeepingMemory() const noexcept { return _runlog.Memory(); }

 protected:
  PostingLogBase(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : _term_ids{blocks}, _runlog{blocks, rm} {}

  LogColumn _term_ids;
  RunLog _runlog;
};

// Positions: almost all tokenizers emit increment 1, making a position equal
// to the within-doc ordinal -- such docs skip the pos column entirely (dense,
// the default). A doc receiving explicit positions is promoted; if it already
// holds dense tokens the implied ramp is backfilled first, so the pos column
// stays aligned for promoted docs and scatter reconstructs dense docs from
// the within-doc index. An all-dense field never allocates the flag bitmap.
class PostingLogPosBase : public PostingLogBase {
 public:
  bool DocExplicit(size_t doc_idx) const noexcept {
    const auto word = doc_idx >> 6;
    return word < _explicit_words.size() &&
           ((_explicit_words[word] >> (doc_idx & 63)) & 1);
  }

  // Whether the doc opened by the last BeginDoc already carries explicit
  // positions. A dense push must not target such a doc (position ==
  // within-doc ordinal only holds for an all-dense doc), so the caller routes
  // through the explicit Push instead.
  bool CurrentDocExplicit() const noexcept {
    return _runlog.DocCount() && DocExplicit(_runlog.DocCount() - 1);
  }

  const LogColumn& Pos() const noexcept { return _pos; }

  size_t BookkeepingMemory() const noexcept {
    return PostingLogBase::BookkeepingMemory() +
           _explicit_words.capacity() * sizeof(uint64_t);
  }

 protected:
  PostingLogPosBase(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : PostingLogBase{blocks, rm},
      _pos{blocks},
      _explicit_words{ManagedTypedAllocator<uint64_t>{rm}} {}

  void PromoteCurrentDoc() {
    const size_t ord = _runlog.DocCount() - 1;
    const auto word = ord >> 6;
    if (word >= _explicit_words.size()) {
      _explicit_words.resize(word + 1, 0);
    }
    const uint64_t bit = uint64_t{1} << (ord & 63);
    if (_explicit_words[word] & bit) {
      return;
    }
    _explicit_words[word] |= bit;
    for (uint32_t i = 1, k = _runlog.LastTokens(); i <= k; ++i) {
      _pos.Push(i);
    }
  }

  IRS_FORCE_INLINE bool RoutePos(bool dense, uint32_t pos_base, size_t n) {
    if (dense) {
      if (CurrentDocExplicit()) [[unlikely]] {
        // Dense tokens appended to a promoted doc: their implied ramp
        // becomes explicit at the running base.
        _pos.PushRamp(pos_base, n);
      }
      return false;
    }
    PromoteCurrentDoc();
    return true;
  }

  void PushBatchTermsPos(doc_id_t doc, std::span<const uint32_t> term_ids,
                         bool dense, std::span<const uint32_t> pos,
                         uint32_t pos_base) {
    _runlog.BeginDoc(doc);
    _term_ids.PushN(term_ids.data(), term_ids.size());
    if (RoutePos(dense, pos_base, term_ids.size())) {
      SDB_ASSERT(pos.size() == term_ids.size());
      _pos.PushNAdd(pos.data(), pos.size(), pos_base);
    }
    _runlog.AddTokens(static_cast<uint32_t>(term_ids.size()));
  }

  LogColumn _pos;
  ManagedVector<uint64_t> _explicit_words;
};

template<TokenLayout L>
class PostingLog;

template<>
class PostingLog<TokenLayout::Terms> final : public PostingLogBase {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::Terms;

  PostingLog(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : PostingLogBase{blocks, rm} {}

  IRS_FORCE_INLINE void Push(doc_id_t doc, uint32_t term_id) {
    _runlog.BeginDoc(doc);
    _term_ids.Push(term_id);
    _runlog.AddTokens(1);
  }

  void PushBatch(doc_id_t doc, std::span<const uint32_t> term_ids) {
    _runlog.BeginDoc(doc);
    _term_ids.PushN(term_ids.data(), term_ids.size());
    _runlog.AddTokens(static_cast<uint32_t>(term_ids.size()));
  }
};

template<>
class PostingLog<TokenLayout::TermsPos> final : public PostingLogPosBase {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::TermsPos;

  PostingLog(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : PostingLogPosBase{blocks, rm} {}

  void PushBatch(doc_id_t doc, std::span<const uint32_t> term_ids, bool dense,
                 std::span<const uint32_t> pos, uint32_t pos_base) {
    PushBatchTermsPos(doc, term_ids, dense, pos, pos_base);
  }

  // One dense-intent token; the log owns the promoted-doc decision (an
  // explicit doc keeps explicit positions).
  IRS_FORCE_INLINE void PushOne(doc_id_t doc, uint32_t term_id, uint32_t pos) {
    _runlog.BeginDoc(doc);
    _term_ids.Push(term_id);
    RoutePos(true, pos - 1, 1);
    _runlog.AddTokens(1);
  }
};

template<>
class PostingLog<TokenLayout::TermsPosOffs> final : public PostingLogPosBase {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::TermsPosOffs;

  PostingLog(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : PostingLogPosBase{blocks, rm}, _offs_start{blocks}, _offs_len{blocks} {}

  // Ends are taken absolute but stored as span LENGTHS (end - start):
  // lengths are small and rebase-invariant, so min-FOR packs them to a few
  // bits where absolute ends need the full offset width. The scatter
  // reconstructs end = start + len.
  void PushBatch(doc_id_t doc, std::span<const uint32_t> term_ids, bool dense,
                 std::span<const uint32_t> pos, uint32_t pos_base,
                 std::span<const uint32_t> offs_start,
                 std::span<const uint32_t> offs_end, uint32_t offs_base) {
    PushBatchTermsPos(doc, term_ids, dense, pos, pos_base);
    const auto n = term_ids.size();
    SDB_ASSERT(offs_start.size() == n);
    SDB_ASSERT(offs_end.size() == n);
    _offs_start.PushNAdd(offs_start.data(), n, offs_base);
    _offs_len.PushNSub(offs_end.data(), offs_start.data(), n);
  }

  IRS_FORCE_INLINE void PushOne(doc_id_t doc, uint32_t term_id, uint32_t pos,
                                uint32_t offs_start, uint32_t offs_end) {
    SDB_ASSERT(offs_end >= offs_start);
    _runlog.BeginDoc(doc);
    _term_ids.Push(term_id);
    RoutePos(true, pos - 1, 1);
    _offs_start.Push(offs_start);
    _offs_len.Push(offs_end - offs_start);
    _runlog.AddTokens(1);
  }

  const LogColumn& OffsStart() const noexcept { return _offs_start; }
  const LogColumn& OffsLen() const noexcept { return _offs_len; }

 private:
  LogColumn _offs_start;
  LogColumn _offs_len;
};

using PostingLogVariant = std::variant<PostingLog<TokenLayout::Terms>,
                                       PostingLog<TokenLayout::TermsPos>,
                                       PostingLog<TokenLayout::TermsPosOffs>>;

static_assert(static_cast<size_t>(TokenLayout::Terms) == 0 &&
              static_cast<size_t>(TokenLayout::TermsPos) == 1 &&
              static_cast<size_t>(TokenLayout::TermsPosOffs) == 2);

inline PostingLogVariant MakePostingLog(duckdb::ArenaAllocator& blocks,
                                        IResourceManager& rm,
                                        TokenLayout layout) {
  switch (layout) {
    case TokenLayout::Terms:
      return PostingLogVariant{
        std::in_place_type<PostingLog<TokenLayout::Terms>>, blocks, rm};
    case TokenLayout::TermsPos:
      return PostingLogVariant{
        std::in_place_type<PostingLog<TokenLayout::TermsPos>>, blocks, rm};
    case TokenLayout::TermsPosOffs:
      return PostingLogVariant{
        std::in_place_type<PostingLog<TokenLayout::TermsPosOffs>>, blocks, rm};
  }
  SDB_UNREACHABLE();
}

}  // namespace irs
