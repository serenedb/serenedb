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

#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/inverter/packed_column.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/noncopyable.hpp"

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

class RunLog {
 public:
  struct Run {
    doc_id_t first_doc;
    uint32_t ndocs;
  };

  RunLog(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : _runs{ManagedTypedAllocator<Run>{rm}}, _doc_tokens{blocks, rm} {}

  IRS_FORCE_INLINE void BeginDoc(doc_id_t doc) {
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

  void AddTokens(uint32_t n) { _doc_tokens.IncBack(n); }

  size_t DocCount() const noexcept { return _doc_tokens.Size(); }
  uint32_t LastTokens() const noexcept { return _doc_tokens.Back(); }

  std::span<const Run> Runs() const noexcept {
    return {_runs.data(), _runs.size()};
  }
  const LogColumn& DocTokens() const noexcept { return _doc_tokens; }

  size_t Memory() const noexcept {
    return _runs.capacity() * sizeof(Run) + _doc_tokens.Memory();
  }

 private:
  static constexpr doc_id_t kMaxBridgedGap = 7;

  ManagedVector<Run> _runs;
  LogColumn _doc_tokens;
};

struct LogEntry {
  uint32_t begin;
  uint32_t count;
  uint32_t len;
  uint32_t refs;
};

class PostingLogBase : util::Noncopyable {
 public:
  uint64_t Size() const noexcept { return _term_ids.Size() + _ref_tokens; }

  std::span<const RunLog::Run> Runs() const noexcept { return _runlog.Runs(); }
  const LogColumn& DocTokens() const noexcept { return _runlog.DocTokens(); }
  const LogColumn& TermIds() const noexcept { return _term_ids; }

  bool DocRef(size_t doc_idx) const noexcept {
    const auto word = doc_idx >> 6;
    return word < _ref_words.size() &&
           ((_ref_words[word] >> (doc_idx & 63)) & 1);
  }
  const LogColumn& Refs() const noexcept { return _refs; }
  std::span<const LogEntry> Entries() const noexcept {
    return {_entries.data(), _entries.size()};
  }
  std::span<const uint32_t> EntryIds() const noexcept {
    return {_entry_ids.data(), _entry_ids.size()};
  }

  uint32_t EntryCount() const noexcept {
    return static_cast<uint32_t>(_entries.size());
  }
  LogEntry& EntryAt(uint32_t e) noexcept { return _entries[e]; }

  void AddEntries(uint32_t n) { _entries.resize(_entries.size() + n); }

  void OpenEntry(uint32_t e) noexcept {
    _entries[e].begin = static_cast<uint32_t>(_entry_ids.size());
  }

  IRS_FORCE_INLINE void PushRef(doc_id_t doc, uint32_t e) {
    auto& entry = _entries[e];
    _runlog.BeginDoc(doc);
    SDB_ASSERT(_runlog.LastTokens() == 0);
    _runlog.AddTokens(entry.count);
    const size_t ord = _runlog.DocCount() - 1;
    const auto word = ord >> 6;
    if (word >= _ref_words.size()) {
      _ref_words.resize(word + 1, 0);
    }
    _ref_words[word] |= uint64_t{1} << (ord & 63);
    _refs.Push(e);
    ++entry.refs;
    _ref_tokens += entry.count;
  }

  size_t BookkeepingMemory() const noexcept {
    return _runlog.Memory() + _term_ids.Memory() + _refs.Memory() +
           _ref_words.capacity() * sizeof(uint64_t) +
           _entries.capacity() * sizeof(LogEntry) +
           _entry_ids.capacity() * sizeof(uint32_t);
  }

 protected:
  PostingLogBase(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : _term_ids{blocks, rm},
      _runlog{blocks, rm},
      _refs{blocks, rm},
      _ref_words{ManagedTypedAllocator<uint64_t>{rm}},
      _entries{ManagedTypedAllocator<LogEntry>{rm}},
      _entry_ids{ManagedTypedAllocator<uint32_t>{rm}} {}

  void AppendEntryIds(uint32_t e, const uint32_t* ids, uint32_t n) {
    _entry_ids.insert(_entry_ids.end(), ids, ids + n);
    _entries[e].count += n;
  }

  LogColumn _term_ids;
  RunLog _runlog;
  LogColumn _refs;
  ManagedVector<uint64_t> _ref_words;
  ManagedVector<LogEntry> _entries;
  ManagedVector<uint32_t> _entry_ids;
  uint64_t _ref_tokens = 0;
};

class PostingLogPosBase : public PostingLogBase {
 public:
  bool DocExplicit(size_t doc_idx) const noexcept {
    const auto word = doc_idx >> 6;
    return word < _explicit_words.size() &&
           ((_explicit_words[word] >> (doc_idx & 63)) & 1);
  }

  bool CurrentDocExplicit() const noexcept {
    return _runlog.DocCount() && DocExplicit(_runlog.DocCount() - 1);
  }

  const LogColumn& Pos() const noexcept { return _pos; }
  bool EntryPosDense() const noexcept { return _entry_pos_dense; }
  std::span<const uint32_t> EntryPos() const noexcept {
    return {_entry_pos.data(), _entry_pos.size()};
  }
  uint32_t EntryLastPos(const LogEntry& entry) const noexcept {
    SDB_ASSERT(entry.count != 0);
    return _entry_pos_dense ? entry.count
                            : _entry_pos[entry.begin + entry.count - 1];
  }

  size_t BookkeepingMemory() const noexcept {
    return PostingLogBase::BookkeepingMemory() + _pos.Memory() +
           _explicit_words.capacity() * sizeof(uint64_t) +
           _entry_pos.capacity() * sizeof(uint32_t);
  }

  void AppendEntry(uint32_t e, const uint32_t* ids, uint32_t n,
                   const uint32_t* pos, uint32_t dense_from) {
    if (pos && _entry_pos_dense) {
      _entry_pos.reserve(_entry_ids.size() + n);
      for (const auto& entry : _entries) {
        for (uint32_t i = 1; i <= entry.count; ++i) {
          _entry_pos.push_back(i);
        }
      }
      _entry_pos_dense = false;
    }
    if (pos) {
      _entry_pos.insert(_entry_pos.end(), pos, pos + n);
    } else if (!_entry_pos_dense) {
      for (uint32_t i = 1; i <= n; ++i) {
        _entry_pos.push_back(dense_from + i);
      }
    }
    AppendEntryIds(e, ids, n);
  }

 protected:
  PostingLogPosBase(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : PostingLogBase{blocks, rm},
      _pos{blocks, rm},
      _explicit_words{ManagedTypedAllocator<uint64_t>{rm}},
      _entry_pos{ManagedTypedAllocator<uint32_t>{rm}} {}

  IRS_FORCE_INLINE void PushPos(uint32_t abs) {
    _pos.Push(abs - _pos_prev);
    _pos_prev = abs;
  }

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
    _pos_prev = 0;
    for (uint32_t i = 1, k = _runlog.LastTokens(); i <= k; ++i) {
      PushPos(i);
    }
  }

  enum class PosRoute {
    Skip,
    WriteExplicit,
  };

  IRS_FORCE_INLINE PosRoute RoutePos(bool dense, uint32_t pos_base, size_t n) {
    if (dense) {
      if (CurrentDocExplicit()) [[unlikely]] {
        for (size_t i = 1; i <= n; ++i) {
          PushPos(pos_base + static_cast<uint32_t>(i));
        }
      }
      return PosRoute::Skip;
    }
    PromoteCurrentDoc();
    return PosRoute::WriteExplicit;
  }

  IRS_FORCE_INLINE void PushOne(doc_id_t doc, uint32_t term_id, uint32_t pos) {
    _runlog.BeginDoc(doc);
    _term_ids.Push(term_id);
    RoutePos(true, pos - 1, 1);
    _runlog.AddTokens(1);
  }

  void PushBatch(doc_id_t doc, std::span<const uint32_t> term_ids, bool dense,
                 std::span<const uint32_t> pos, uint32_t pos_base) {
    _runlog.BeginDoc(doc);
    _term_ids.PushN(term_ids.data(), term_ids.size());
    if (RoutePos(dense, pos_base, term_ids.size()) == PosRoute::WriteExplicit) {
      SDB_ASSERT(pos.size() == term_ids.size());
      _pos_prev = _pos.PushNDelta(pos.data(), pos.size(), pos_base, _pos_prev);
    }
    _runlog.AddTokens(static_cast<uint32_t>(term_ids.size()));
  }

  LogColumn _pos;
  ManagedVector<uint64_t> _explicit_words;
  ManagedVector<uint32_t> _entry_pos;
  uint32_t _pos_prev = 0;
  bool _entry_pos_dense = true;
};

template<TokenLayout L>
class PostingLog;

template<>
class PostingLog<TokenLayout::Terms> final : public PostingLogBase {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::Terms;

  PostingLog(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : PostingLogBase{blocks, rm} {}

  IRS_FORCE_INLINE void PushOne(doc_id_t doc, uint32_t term_id) {
    _runlog.BeginDoc(doc);
    _term_ids.Push(term_id);
    _runlog.AddTokens(1);
  }

  void PushBatch(doc_id_t doc, std::span<const uint32_t> term_ids) {
    _runlog.BeginDoc(doc);
    _term_ids.PushN(term_ids.data(), term_ids.size());
    _runlog.AddTokens(static_cast<uint32_t>(term_ids.size()));
  }

  void AppendEntry(uint32_t e, const uint32_t* ids, uint32_t n) {
    AppendEntryIds(e, ids, n);
  }
};

template<>
class PostingLog<TokenLayout::TermsPos> final : public PostingLogPosBase {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::TermsPos;

  PostingLog(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : PostingLogPosBase{blocks, rm} {}

  using PostingLogPosBase::PushBatch;
  using PostingLogPosBase::PushOne;
};

template<>
class PostingLog<TokenLayout::TermsPosOffs> final : public PostingLogPosBase {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::TermsPosOffs;

  PostingLog(duckdb::ArenaAllocator& blocks, IResourceManager& rm)
    : PostingLogPosBase{blocks, rm},
      _offs_delta{blocks, rm},
      _offs_len{blocks, rm},
      _entry_offs_start{ManagedTypedAllocator<uint32_t>{rm}},
      _entry_offs_end{ManagedTypedAllocator<uint32_t>{rm}} {}

  size_t BookkeepingMemory() const noexcept {
    return PostingLogPosBase::BookkeepingMemory() + _offs_delta.Memory() +
           _offs_len.Memory() +
           (_entry_offs_start.capacity() + _entry_offs_end.capacity()) *
             sizeof(uint32_t);
  }

  std::span<const uint32_t> EntryOffsStart() const noexcept {
    return {_entry_offs_start.data(), _entry_offs_start.size()};
  }
  std::span<const uint32_t> EntryOffsEnd() const noexcept {
    return {_entry_offs_end.data(), _entry_offs_end.size()};
  }

  void AppendEntry(uint32_t e, const uint32_t* ids, uint32_t n,
                   const uint32_t* pos, uint32_t dense_from,
                   const uint32_t* offs_start, const uint32_t* offs_end) {
    _entry_offs_start.insert(_entry_offs_start.end(), offs_start,
                             offs_start + n);
    _entry_offs_end.insert(_entry_offs_end.end(), offs_end, offs_end + n);
    PostingLogPosBase::AppendEntry(e, ids, n, pos, dense_from);
  }

  void PushBatch(doc_id_t doc, std::span<const uint32_t> term_ids, bool dense,
                 std::span<const uint32_t> pos, uint32_t pos_base,
                 std::span<const uint32_t> offs_start,
                 std::span<const uint32_t> offs_end, uint32_t offs_base) {
    PostingLogPosBase::PushBatch(doc, term_ids, dense, pos, pos_base);
    const auto n = term_ids.size();
    SDB_ASSERT(offs_start.size() == n);
    SDB_ASSERT(offs_end.size() == n);
    OpenOffsDoc(doc);
    _offs_prev =
      _offs_delta.PushNDelta(offs_start.data(), n, offs_base, _offs_prev);
    _offs_len.PushNSub(offs_end.data(), offs_start.data(), n);
  }

  IRS_FORCE_INLINE void PushOne(doc_id_t doc, uint32_t term_id, uint32_t pos,
                                uint32_t offs_start, uint32_t offs_end) {
    SDB_ASSERT(offs_end >= offs_start);
    PostingLogPosBase::PushOne(doc, term_id, pos);
    OpenOffsDoc(doc);
    _offs_delta.Push(offs_start - _offs_prev);
    _offs_prev = offs_start;
    _offs_len.Push(offs_end - offs_start);
  }

  const LogColumn& OffsDelta() const noexcept { return _offs_delta; }
  const LogColumn& OffsLen() const noexcept { return _offs_len; }

 private:
  void OpenOffsDoc(doc_id_t doc) noexcept {
    if (doc != _offs_doc) {
      _offs_prev = 0;
      _offs_doc = doc;
    }
  }

  LogColumn _offs_delta;
  LogColumn _offs_len;
  ManagedVector<uint32_t> _entry_offs_start;
  ManagedVector<uint32_t> _entry_offs_end;
  doc_id_t _offs_doc{doc_limits::invalid()};
  uint32_t _offs_prev = 0;
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
  return ResolveLayout(layout, [&]<TokenLayout L>() {
    return PostingLogVariant{std::in_place_type<PostingLog<L>>, blocks, rm};
  });
}

}  // namespace irs
