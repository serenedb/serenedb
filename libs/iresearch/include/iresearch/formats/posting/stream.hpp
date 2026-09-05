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

#include <iterator>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

// One term's posting list read front to back, which is the whole of what
// re-writing it needs: the block after the one in hand is always the next
// one, so there is no skip list here and nothing seeks. `IteratorTraits` says
// what is decoded, `FieldTraits` what is stepped over.
template<typename IteratorTraits, typename FieldTraits, typename InputType>
class PostingsStream : public TermPostings {
  static_assert((IteratorTraits::Features() & FieldTraits::Features()) ==
                IteratorTraits::Features());

 public:
  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               const IndexInput* pos_in, const IndexInput* pay_in,
               bool has_score_bounds) {
    SDB_ASSERT(meta.docs_count != 0);

    if (meta.docs_count == 1) {
      const auto doc = doc_limits::min() + meta.doc_delta;
      *(std::end(_docs) - 1) = doc;
      if constexpr (IteratorTraits::Frequency()) {
        *(std::end(_freqs) - 1) = meta.freq;
      }
      _left_in_leaf = 1;
      _max_in_leaf = doc;
    } else {
      _doc_in = doc_in.Reopen();
      if (!_doc_in) [[unlikely]] {
        throw IoError{"failed to reopen document input"};
      }

      auto& in = In();
      in.Seek(meta.doc_start);
      // A term short enough to have no skip list carries its score bound
      // ahead of the one block it does have; a longer one carries it past
      // the blocks, where nothing reading forward ever reaches it.
      if (meta.docs_count < doc_limits::kBlockSize) {
        SkipScoreBounds(has_score_bounds, in);
      }
      _left_in_list = meta.docs_count;
    }

    if constexpr (IteratorTraits::Position()) {
      const DocState state{
        .pos_in = pos_in,
        .pay_in = pay_in,
        .term_state = &meta,
        .enc_buf = _enc_buf,
      };
      _pos.template Prepare<InputType>(state);
    }
  }

  doc_id_t Advance() final {
    if (_left_in_leaf == 0) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      ReadLeaf(_max_in_leaf);
    }

    if constexpr (IteratorTraits::Position()) {
      const auto freq = *(std::end(_freqs) - _left_in_leaf);
      _pos.Notify(freq, freq);
      _pos.Clear();
    }

    _doc = *(std::end(_docs) - _left_in_leaf);
    --_left_in_leaf;
    return _doc;
  }

  uint32_t GetFreq() const final {
    if constexpr (IteratorTraits::Frequency()) {
      SDB_ASSERT(_left_in_leaf < doc_limits::kBlockSize);
      return *(std::end(_freqs) - _left_in_leaf - 1);
    } else {
      return 0;
    }
  }

  PosAttr* Positions() noexcept final {
    if constexpr (IteratorTraits::Position()) {
      return &_pos;
    } else {
      return nullptr;
    }
  }

 private:
  using Position = PositionImpl<IteratorTraits>;

  IRS_FORCE_INLINE InputType& In() const noexcept {
    return sdb::basics::downCast<InputType>(*_doc_in);
  }

  void ReadLeaf(doc_id_t prev) {
    auto& in = In();
    if (_left_in_list >= doc_limits::kBlockSize) [[likely]] {
      IteratorTraits::ReadBlockDelta(in, _enc_buf, _docs, prev);
      _left_in_leaf = doc_limits::kBlockSize;
      _left_in_list -= doc_limits::kBlockSize;
      ReadLeafFreqs(doc_limits::kBlockSize);
    } else {
      const auto tail = _left_in_list;
      IteratorTraits::ReadTailDelta(tail, in, _enc_buf, _docs, prev);
      _left_in_leaf = tail;
      _left_in_list = 0;
      ReadLeafFreqs(tail);
    }
    _max_in_leaf = *(std::end(_docs) - 1);
  }

  void ReadLeafFreqs(uint32_t len) {
    if constexpr (IteratorTraits::Frequency()) {
      IteratorTraits::ReadTail(len, In(), _enc_buf, _freqs);
    } else if constexpr (FieldTraits::Frequency()) {
      // Only a full block is followed by more of this term's documents, so
      // only a full block has to be stepped over.
      if (len == doc_limits::kBlockSize) {
        FieldTraits::SkipBlock(In());
      }
    }
  }

  ABSL_CACHELINE_ALIGNED uint32_t _enc_buf[doc_limits::kBlockSize];
  [[no_unique_address]] ABSL_CACHELINE_ALIGNED utils::Need<
    IteratorTraits::Frequency(), uint32_t[doc_limits::kBlockSize]> _freqs;
  ABSL_CACHELINE_ALIGNED doc_id_t _docs[doc_limits::kBlockSize];
#ifdef __AVX2__
  [[maybe_unused]] doc_id_t
    _placeholder_for_bitset_materialize[doc_limits::kRunSlack];
#endif
  IndexInput::ptr _doc_in;
  [[no_unique_address]] utils::Need<IteratorTraits::Position(), Position> _pos;
  doc_id_t _max_in_leaf = doc_limits::invalid();
  uint32_t _left_in_leaf = 0;
  uint32_t _left_in_list = 0;
};

}  // namespace irs
