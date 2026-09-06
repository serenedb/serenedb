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

#include <algorithm>
#include <vector>

#include "basics/down_cast.h"
#include "basics/shared.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/formats/posting/skip_list.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/enc_buf.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::search {

template<bool Offs>
struct SkipCopyTraits {
  static constexpr bool Position() noexcept { return true; }
  static constexpr bool Offset() noexcept { return Offs; }
};

template<typename InputType, bool Bounds, bool Offs = false>
class PostingPos {
 public:
  using PosTraits = IteratorTraitsImpl<FormatTraits128, true, true, Offs>;
  using Position = PositionImpl<PosTraits>;

  static constexpr bool kOffsets = Offs;

  PostingPos() : _skip{doc_limits::kBlockSize, doc_limits::kSkipSize} {}

  PostingPos(const PostingPos&) = delete;
  PostingPos& operator=(const PostingPos&) = delete;
  PostingPos(PostingPos&&) = delete;
  PostingPos& operator=(PostingPos&&) = delete;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const IndexInput& pos_in,
               const IndexInput* pay_in) {
    SDB_ASSERT(meta.docs_count != 0);
    const auto skip = ToSkipLayout(layout);
    SDB_ASSERT(skip.pos);
    SDB_ASSERT(!Offs || skip.offs);
    _skip.Reader().SetLayout(skip);
    _docs_count = meta.docs_count;

    if (meta.docs_count == 1) {
      const auto doc = doc_limits::min() + meta.doc_delta;
      *(std::end(_docs) - 1) = doc;
      *(std::end(_freqs.data) - 1) = meta.freq;
      _left_in_leaf = 1;
      _max_in_leaf = doc;
    } else {
      _in = doc_in.Reopen();
      if (!_in) [[unlikely]] {
        throw IoError{"failed to reopen document input"};
      }
      auto& in = In();
      in.Seek(meta.doc_start);
      if (meta.docs_count < doc_limits::kBlockSize) {
        SkipScoreBounds(Bounds, in);
      }
      _left_in_list = meta.docs_count;

      if (meta.docs_count > doc_limits::kBlockSize) {
        _skip.Reader().Enable(meta);
        _skip_offs = meta.doc_start + meta.doc_delta;
        _pending_skip = meta.docs_count;
      }
    }

    const DocState state{
      .pos_in = &pos_in,
      .pay_in = pay_in,
      .term_state = &meta,
      .enc_buf = Enc(),
    };
    _pos.template Prepare<InputType>(state);
  }

  doc_id_t Value() const noexcept { return _doc; }

  const doc_id_t& ValueRef() const noexcept { return _doc; }

  uint32_t Estimate() const noexcept { return _docs_count; }

  Position& Positions() noexcept { return _pos; }

  IRS_FORCE_INLINE doc_id_t Advance() {
    if (_left_in_leaf == 0) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      ReadLeaf(_max_in_leaf);
    }
    const auto freq = *(std::end(_freqs.data) - _left_in_leaf);
    _doc = *(std::end(_docs) - _left_in_leaf);
    --_left_in_leaf;
    _pos.Notify(freq, freq);
    _pos.Clear();
    return _doc;
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }

    if (_max_in_leaf < target && !SeekToLeaf(target)) [[unlikely]] {
      _left_in_leaf = 0;
      return _doc = doc_limits::eof();
    }

    static constexpr uint32_t kGroup = 8;

    auto left = _left_in_leaf;
    const auto* doc = std::end(_docs) - left;
    const auto* freq = std::end(_freqs.data) - left;
    uint32_t notify = 0;

    const auto found = [&](uint32_t i) IRS_FORCE_INLINE {
      _pos.Notify(freq[i], notify + freq[i]);
      _pos.Clear();
      _left_in_leaf = left - i - 1;
      return _doc = doc[i];
    };

    for (; left >= kGroup; left -= kGroup, doc += kGroup, freq += kGroup) {
      uint32_t below = 0;
      uint32_t skipped = 0;
      for (uint32_t i = 0; i != kGroup; ++i) {
        const auto lower = static_cast<uint32_t>(doc[i] < target);
        below += lower;
        skipped += freq[i] * lower;
      }
      notify += skipped;
      if (below != kGroup) {
        return found(below);
      }
    }

    for (uint32_t i = 0; i != left; ++i) {
      if (target <= doc[i]) {
        return found(i);
      }
      notify += freq[i];
    }

    _left_in_leaf = 0;
    return _doc = doc_limits::eof();
  }

  IRS_FORCE_INLINE doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }

    if (_max_in_leaf < target && !SeekToLeaf(target)) [[unlikely]] {
      _left_in_leaf = 0;
      return _doc = doc_limits::eof();
    }

    const auto left = _left_in_leaf;
    const auto* const doc = std::end(_docs) - left;
    const auto* const freq = std::end(_freqs.data) - left;
    uint32_t notify = 0;
    for (uint32_t i = 0; i != left; ++i) {
      notify += freq[i];
      if (target <= doc[i]) {
        _pos.Notify(freq[i], notify);
        _pos.Clear();
        _left_in_leaf = left - i - 1;
        return _doc = doc[i];
      }
    }

    _left_in_leaf = 0;
    return _doc = doc_limits::eof();
  }

 private:
  IRS_FORCE_INLINE uint32_t* Enc() noexcept { return EncOf<InputType>(_enc); }

  class ReadSkip {
   public:
    ReadSkip() { Disable(); }

    void Disable() noexcept {
      SDB_ASSERT(!doc_limits::valid(_levels[_num_levels - 1].doc));
      _levels[_num_levels - 1].doc = doc_limits::eof();
    }

    void Enable(const PostingMeta& meta) noexcept {
      CopyState<SkipCopyTraits<Offs>>(_levels[0], meta);
      SDB_ASSERT(doc_limits::eof(_levels[_num_levels - 1].doc));
      _levels[_num_levels - 1].doc = doc_limits::invalid();
    }

    void Init(size_t num_levels) {
      SDB_ASSERT(0 < num_levels && num_levels <= doc_limits::kMaxSkipLevels);
      _num_levels = static_cast<uint32_t>(num_levels);
    }

    IRS_FORCE_INLINE bool IsLess(size_t level, doc_id_t target) const noexcept {
      return _levels[level].doc < target;
    }

    void MoveDown(size_t level) noexcept {
      SDB_ASSERT(_prev);
      CopyState<SkipCopyTraits<Offs>>(_levels[level], *_prev);
    }

    void SetLayout(SkipLayout layout) noexcept { _layout = layout; }

    void Read(size_t level, InputType& in) {
      auto& next = _levels[level];
      CopyState<SkipCopyTraits<Offs>>(*_prev, next);
      ReadPosState<Offs>(next, in, _layout.offs);
      SkipScoreBounds(Bounds, in);
    }

    void Seal(size_t level) {
      auto& next = _levels[level];
      CopyState<SkipCopyTraits<Offs>>(*_prev, next);
      next.doc = doc_limits::eof();
    }

    IRS_FORCE_INLINE static size_t AdjustLevel(size_t level) noexcept {
      return level;
    }

    void Reset(SkipState& state) noexcept { _prev = &state; }

    IRS_FORCE_INLINE doc_id_t UpperBound() const noexcept {
      return _levels[_num_levels - 1].doc;
    }

    IRS_FORCE_INLINE void SkipBounds(InputType& in) {
      SkipScoreBounds(Bounds, in);
    }

   private:
    SkipState _levels[doc_limits::kMaxSkipLevels];
    uint32_t _num_levels = 1;
    SkipState* _prev = nullptr;
    SkipLayout _layout;
  };

  IRS_FORCE_INLINE InputType& In() const noexcept {
    return sdb::basics::downCast<InputType>(*_in);
  }

  void ReadLeaf(doc_id_t prev) {
    auto& in = In();
    if (_left_in_list >= doc_limits::kBlockSize) [[likely]] {
      FormatTraits128::ReadBlockDelta(in, Enc(), _docs, prev);
      _left_in_leaf = doc_limits::kBlockSize;
      _left_in_list -= doc_limits::kBlockSize;
      FormatTraits128::ReadBlock(in, Enc(), _freqs.data);
    } else {
      const auto tail = _left_in_list;
      FormatTraits128::ReadTailDelta(tail, in, Enc(), _docs, prev);
      _left_in_leaf = tail;
      _left_in_list = 0;
      FormatTraits128::ReadTail(tail, in, Enc(), _freqs.data);
    }
    _max_in_leaf = *(std::end(_docs) - 1);
  }

  IRS_NO_INLINE bool SeekToLeaf(doc_id_t target) {
    if (target <= _skip.Reader().UpperBound()) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return false;
      }
      ReadLeaf(_max_in_leaf);
      return true;
    }

    SkipState last;
    _skip.Reader().Reset(last);
    if (_pending_skip != 0) [[unlikely]] {
      return InitAndSeek(last, target);
    }
    return SeekAfterInit(last, target);
  }

  IRS_NO_INLINE bool InitAndSeek(SkipState& last, doc_id_t target) {
    std::unique_ptr<InputType> skip_in{
      sdb::basics::downCast<InputType>(_in->Dup().release())};
    if (!skip_in) [[unlikely]] {
      throw IoError{"failed to duplicate document input"};
    }
    skip_in->Seek(_skip_offs);
    _skip.Reader().SkipBounds(*skip_in);
    const auto docs_count = _pending_skip;
    _pending_skip = 0;
    _skip.Prepare(std::move(skip_in), docs_count);

    const auto num_levels = _skip.NumLevels();
    SDB_ENSURE(1 <= num_levels && num_levels <= doc_limits::kMaxSkipLevels,
               "Invalid number of skip levels ", num_levels,
               ", must be in range of [1, ", doc_limits::kMaxSkipLevels, "].");
    _skip.Reader().Init(num_levels);

    return SeekAfterInit(last, target);
  }

  bool SeekAfterInit(SkipState& last, doc_id_t target) {
    _left_in_list = _skip.Seek(target);
    if (_left_in_list == 0) [[unlikely]] {
      return false;
    }
    In().Seek(last.doc_ptr);
    _pos.template Prepare<InputType>(last);
    ReadLeaf(last.doc);
    return true;
  }

  [[no_unique_address]] NeedEnc<InputType> _enc;
  FreqBuf _freqs;
  DocsBuf _docs;
  IndexInput::ptr _in;
  Position _pos;
  SkipReader<ReadSkip, InputType> _skip;
  uint64_t _skip_offs = 0;
  doc_id_t _doc = doc_limits::invalid();
  doc_id_t _max_in_leaf = doc_limits::invalid();
  uint32_t _docs_count = 0;
  uint32_t _left_in_leaf = 0;
  uint32_t _left_in_list = 0;
  uint32_t _pending_skip = 0;
};

}  // namespace irs::search
