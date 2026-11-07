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

#include "iresearch/formats/posting/block_index.hpp"
#include "iresearch/formats/posting/block_io.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/doc_input.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/enc_buf.hpp"
#include "iresearch/search/detail/posting_leaf.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename InputType, bool Bounds, bool Offs = false,
         bool Deferrable = false>
class PostingPos : public PostingLeaf<InputType, kPosShape> {
  static_assert(!Deferrable || InputType::kVolatileAlways);

  using Base = PostingLeaf<InputType, kPosShape>;
  using Base::_doc;
  using Base::_docs;
  using Base::_freqs;
  using Base::_hint;
  using Base::_last;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::_walk;
  using Base::Enc;
  using Base::In;
  using Base::ReadLeafDelta;

 public:
  using PosTraits = IteratorTraitsImpl<true, true, Offs>;
  using Position = PositionImpl<PosTraits>;

  static constexpr bool kOffsets = Offs;

  PostingPos() = default;

  explicit PostingPos(const PostingMeta& meta, const IndexInput& doc_in,
                      IndexFeatures layout, const IndexInput& pos_in,
                      const IndexInput* pay_in) {
    Prepare(meta, doc_in, layout, pos_in, pay_in);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const IndexInput& pos_in,
               const IndexInput* pay_in) {
    SDB_ASSERT(IsSubsetOf(IndexFeatures::Pos, layout));
    SDB_ASSERT(!Offs || IsSubsetOf(IndexFeatures::Offs, layout));
    _docs_count = meta.docs_count;
    if (!this->OpenList(meta, doc_in, layout, Bounds)) {
      this->SetSingle(meta);
    }
    PreparePositions(_pos, meta, pos_in, pay_in);
  }

  void PreparePositions(Position& pos, const PostingMeta& meta,
                        const IndexInput& pos_in, const IndexInput* pay_in) {
    const DocState state{
      .pos_in = &pos_in,
      .pay_in = pay_in,
      .term_state = &meta,
      .enc_buf = Enc(),
    };
    pos.template Prepare<InputType>(state);
  }

  doc_id_t Value() const noexcept { return _doc; }

  const doc_id_t& ValueRef() const noexcept { return _doc; }

  uint32_t Estimate() const noexcept { return _docs_count; }

  bool Covers(doc_id_t target) const noexcept { return target <= _last; }

  std::span<const doc_id_t> Upcoming() const noexcept {
    return {std::cend(_docs) - _left_in_leaf, _left_in_leaf};
  }

  std::span<const doc_id_t> Onward() const noexcept {
    const auto* const current = std::cend(_docs) - _left_in_leaf - 1;
    SDB_ASSERT(*current == _doc);
    return {current, _left_in_leaf + 1};
  }

  Position& Positions() noexcept { return _pos; }

  const BlockIndex* Blocks() {
    if constexpr (Bounds) {
      if (_walk.Armed()) {
        return &_walk.Loaded(In());
      }
    }
    return nullptr;
  }

  uint32_t LeafBlock() const noexcept {
    SDB_ASSERT(_left_in_list < _docs_count);
    return (_docs_count - _left_in_list - 1) / doc_limits::kBlockSize;
  }

  void Defer() noexcept
    requires(Deferrable)
  {
    _deferral.on = _docs_count > doc_limits::kBlockSize;
  }

  bool Deferred() const noexcept
    requires(Deferrable)
  {
    return _deferral.on;
  }

  IRS_FORCE_INLINE void Sync()
    requires(Deferrable)
  {
    const auto& deferral = _deferral;
    if (deferral.on && deferral.notified + _left_in_leaf < deferral.len) {
      SyncDeferred();
    }
  }

  IRS_FORCE_INLINE doc_id_t ProbeDeferred(doc_id_t target)
    requires(Deferrable)
  {
    SDB_ASSERT(_deferral.on);
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    if (_last < target && !LandDeferred(target)) [[unlikely]] {
      _left_in_leaf = 0;
      return _doc = doc_limits::eof();
    }
    return Locate(target);
  }

  IRS_FORCE_INLINE doc_id_t Next() {
    if (_left_in_leaf == 0) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      ReadLeafDelta(_last);
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

    if (_last < target && !SeekToLeaf(target)) [[unlikely]] {
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

    if (_last < target && !SeekToLeaf(target)) [[unlikely]] {
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
  static constexpr uint32_t kEagerShare = 4;
  static constexpr uint32_t kEagerProbes = 64;

  struct Deferral {
    const byte_type* freqs = nullptr;
    uint32_t len = 0;
    uint32_t notified = 0;
    uint32_t probes = 0;
    uint32_t landings = 0;
    uint32_t syncs = 0;
    bool on = false;
  };

  IRS_FORCE_INLINE doc_id_t Locate(doc_id_t target) {
    ++_deferral.probes;
    const auto* const end = std::cend(_docs);
    const auto* const it = LeafLowerBound<doc_limits::kBlockSize>(
      std::cbegin(_docs), _deferral.len, end - _left_in_leaf, target);
    _left_in_leaf = static_cast<uint32_t>(end - it) - 1;
    return _doc = *it;
  }

  IRS_NO_INLINE void SyncDeferred() {
    auto& deferral = _deferral;
    const auto len = deferral.len;
    if (deferral.freqs != nullptr) [[unlikely]] {
      DecodeDeferred();
    }
    const auto current = len - _left_in_leaf - 1;
    const auto* const freq = std::cend(_freqs.data) - len;
    uint32_t skipped = 0;
    for (auto i = deferral.notified; i != current; ++i) {
      skipped += freq[i];
    }
    _pos.Notify(freq[current], skipped + freq[current]);
    _pos.Clear();
    deferral.notified = current + 1;
    const auto syncs = uint64_t{++deferral.syncs} * kEagerShare;
    const uint64_t probes = deferral.probes;
    if (probes >= kEagerProbes &&
        (syncs > 2 * probes ||
         (syncs > probes && deferral.landings * kEagerShare < probes)))
      [[unlikely]] {
      deferral.on = false;
    }
  }

  IRS_NO_INLINE void DecodeDeferred() noexcept {
    block_io::DecodeTail<block_io::kFreqBias>(_deferral.freqs, _deferral.len,
                                              _freqs.data);
    _deferral.freqs = nullptr;
  }

  IRS_NO_INLINE bool LandDeferred(doc_id_t target) {
    ++_deferral.landings;
    _left_in_list = _walk.Seek(target, In());
    if (_left_in_list == 0) [[unlikely]] {
      return false;
    }
    const auto last = _walk.Landing();
    auto& in = In();
    in.Seek(last.doc_ptr);
    _pos.Prepare(last);
    _hint.Advance(in, in.Position());
    const auto len = std::min(_left_in_list, doc_limits::kBlockSize);
    block_io::ReadTailDelta(len, in, Enc(), _docs, last.doc);
    _deferral.freqs = in.Current();
    block_io::SkipTail(len, in);
    _left_in_leaf = len;
    _left_in_list -= len;
    _deferral.len = len;
    _deferral.notified = 0;
    _last = *(std::end(_docs) - 1);
    return true;
  }

  IRS_NO_INLINE bool SeekToLeaf(doc_id_t target) {
    if (target <= _walk.UpperBound()) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return false;
      }
      ReadLeafDelta(_last);
      return true;
    }

    _left_in_list = _walk.Seek(target, In());
    if (_left_in_list == 0) [[unlikely]] {
      return false;
    }
    const auto last = _walk.Landing();
    In().Seek(last.doc_ptr);
    _pos.Prepare(last);
    ReadLeafDelta(last.doc);
    return true;
  }

  Position _pos;
  uint32_t _docs_count = 0;
  [[no_unique_address]] utils::Need<Deferrable, Deferral> _deferral;
};

}  // namespace irs::detail
