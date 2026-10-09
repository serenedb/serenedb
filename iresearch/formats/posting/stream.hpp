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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/formats/posting/block_io.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/doc_input.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

template<typename IteratorTraits, typename FieldTraits, typename InputType>
class PostingsStream : public TermPostings {
  static_assert((IteratorTraits::Features() & FieldTraits::Features()) ==
                IteratorTraits::Features());

 public:
  PostingsStream() noexcept {}

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               const IndexInput* pos_in, const IndexInput* pay_in,
               bool has_score_bounds) {
    SDB_ASSERT(meta.docs_count != 0);

    _max_in_leaf = doc_limits::invalid();
    _left_in_leaf = 0;
    _left_in_list = 0;
    if (meta.docs_count == 1) {
      const auto doc = doc_limits::min() + meta.doc_delta;
      *(std::end(_docs) - 1) = doc;
      if constexpr (IteratorTraits::Frequency()) {
        *(std::end(_freqs) - 1) = meta.freq;
      }
      _left_in_leaf = 1;
      _max_in_leaf = doc;
    } else {
      if (meta.inline_size != 0) {
        _inline_in.reset(meta.Inline());
        _doc_in = &_inline_in;
      } else {
        if (!_file_in) {
          _file_in = doc_in.Reopen();
          if (!_file_in) [[unlikely]] {
            throw IoError{"failed to reopen document input"};
          }
        }
        _file_in->Seek(meta.doc_start);
        _doc_in = _file_in.get();
      }

      auto& in = In();
      PrefetchDocs(in, meta);
      if (meta.docs_count <= doc_limits::kBlockSize) {
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

  uint32_t NextDocs(doc_id_t* docs, uint32_t* freqs) final {
    if (_left_in_leaf == 0) {
      if (_left_in_list == 0) {
        return 0;
      }
      ReadLeaf(_max_in_leaf);
    }
    const auto n = _left_in_leaf;
    std::copy_n(std::end(_docs) - n, n, docs);
    if constexpr (IteratorTraits::Frequency()) {
      std::copy_n(std::end(_freqs) - n, n, freqs);
    }
    _left_in_leaf = 0;
    return n;
  }

  void NextPositions(uint32_t* pos, uint32_t* offs_start, uint32_t* offs_len,
                     uint32_t n) final {
    if constexpr (IteratorTraits::Position()) {
      _pos.ReadDeltas(pos, offs_start, offs_len, n);
    } else {
      TermPostings::NextPositions(pos, offs_start, offs_len, n);
    }
  }

  void SkipPositions(uint64_t n) final {
    if constexpr (IteratorTraits::Position()) {
      _pos.SkipDeltas(n);
    } else {
      TermPostings::SkipPositions(n);
    }
  }

 private:
  using Position = PositionImpl<IteratorTraits>;

  IRS_FORCE_INLINE InputType& In() const noexcept {
    return irs::utils::downCast<InputType>(*_doc_in);
  }

  void ReadLeaf(doc_id_t prev) {
    auto& in = In();
    if (_left_in_list >= doc_limits::kBlockSize) [[likely]] {
      block_io::ReadBlockDelta(in, _enc_buf, _docs, prev);
      _left_in_leaf = doc_limits::kBlockSize;
      _left_in_list -= doc_limits::kBlockSize;
      ReadLeafFreqs(doc_limits::kBlockSize);
    } else {
      const auto tail = _left_in_list;
      block_io::ReadTailDelta(tail, in, _enc_buf, _docs, prev);
      _left_in_leaf = tail;
      _left_in_list = 0;
      ReadLeafFreqs(tail);
    }
    _max_in_leaf = *(std::end(_docs) - 1);
  }

  void ReadLeafFreqs(uint32_t len) {
    if constexpr (IteratorTraits::Frequency()) {
      block_io::ReadTail<block_io::kFreqBias>(len, In(), _enc_buf, _freqs);
    } else if constexpr (FieldTraits::Frequency()) {
      // Only a full block is followed by more of this term's documents, so
      // only a full block has to be stepped over.
      if (len == doc_limits::kBlockSize) {
        block_io::SkipBlock(In());
      }
    }
  }

  ABSL_CACHELINE_ALIGNED uint32_t _enc_buf[block_io::kEncWords];
  [[no_unique_address]] ABSL_CACHELINE_ALIGNED utils::Need<
    IteratorTraits::Frequency(),
    SlackBuf<uint32_t, doc_limits::kBlockSize, block_codec::kOutSlack>> _freqs;
  DocsBuf _docs;
  IndexInput::ptr _file_in;
  BytesViewInput _inline_in;
  IndexInput* _doc_in = nullptr;
  [[no_unique_address]] utils::Need<IteratorTraits::Position(), Position> _pos;
  doc_id_t _max_in_leaf = doc_limits::invalid();
  uint32_t _left_in_leaf = 0;
  uint32_t _left_in_list = 0;
};

}  // namespace irs
