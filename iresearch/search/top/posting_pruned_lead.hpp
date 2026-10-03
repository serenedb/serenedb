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

#include "iresearch/search/top/prune_leaf.hpp"

namespace irs::top {

template<typename InputType>
class PostingPrunedLead : public PruneLeafBase<InputType, false> {
  using Base = PruneLeafBase<InputType, false>;

  using Base::_base;
  using Base::_cursor;
  using Base::_doc;
  using Base::_docs;
  using Base::_freqs;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::_max_in_leaf;
  using Base::_needs_reposition;
  using Base::_provider;
  using Base::_score;
  using Base::ReadLeaf;

 public:
  using Base::ForEachScoredBlock;
  using Base::MaxScore;
  using Base::Value;

  PostingPrunedLead() = default;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const SubReader& segment,
               const TermReader& field, const detail::ScoreArgs& args) {
    if (Base::PrepareCommon(meta, doc_in, layout, segment, field, args)) {
      _left_in_leaf = 1;
    }
  }

  PostingPrunedLead(const PostingMeta& meta, const IndexInput& doc_in,
                    IndexFeatures layout, const SubReader& segment,
                    const TermReader& field, const detail::ScoreArgs& args) {
    Prepare(meta, doc_in, layout, segment, field, args);
  }

  doc_id_t BlockLast() {
    Base::SeekToBlock(_doc);
    return *(std::end(_docs) - 1);
  }

  doc_id_t SpanLast(uint32_t blocks) {
    if (blocks > 1 && _cursor.Armed()) {
      const auto& index = _cursor.Index();
      return index.Last(std::min(index.Size(), _cursor.Block() + blocks) - 1);
    }
    return *(std::end(_docs) - 1);
  }

  IRS_FORCE_INLINE doc_id_t Next() {
    if (_left_in_leaf == 0) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      Base::Reposition();
      ReadLeaf(_doc);
    }
    _doc = *(std::end(_docs) - _left_in_leaf);
    --_left_in_leaf;
    return _doc;
  }

  IRS_FORCE_INLINE doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    if (_left_in_leaf != 0 && target <= _max_in_leaf) [[likely]] {
      return _doc = Scan(target);
    }
    return SeekSlow(target);
  }

  IRS_FORCE_INLINE uint32_t Freq() const noexcept {
    return _freqs.data[doc_limits::kBlockSize - _left_in_leaf - 1];
  }

  void ScoreFreqs(uint32_t* freqs, score_t* scores, uint32_t count) {
    _provider.freq.value = freqs;
    if (count == kScoreBlock) {
      _score.ScoreBlock(scores);
    } else {
      _score.Score(scores, static_cast<scores_size_t>(count));
    }
    _provider.freq.value = _freqs.data;
  }

 private:
  IRS_FORCE_INLINE doc_id_t Scan(doc_id_t target) noexcept {
    for (auto left = _left_in_leaf;; --left) {
      const auto doc = *(std::end(_docs) - left);
      if (target <= doc) {
        _left_in_leaf = left - 1;
        return doc;
      }
    }
  }

  IRS_NO_INLINE doc_id_t SeekSlow(doc_id_t target) {
    if (!_needs_reposition && _left_in_list != 0 &&
        target - _max_in_leaf <= _max_in_leaf - _base) {
      ReadLeaf(_max_in_leaf);
      if (target <= _max_in_leaf) [[likely]] {
        return _doc = Scan(target);
      }
    }
    if (_cursor.UpperBound() < target) [[unlikely]] {
      Base::SeekToBlock(target);
      if (_needs_reposition) {
        _doc = _cursor.Landing().doc;
      }
    }
    if (_left_in_leaf == 0) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      Base::Reposition();
      ReadLeaf(_doc);
    }
    for (;;) {
      while (_left_in_leaf != 0) {
        const auto doc = *(std::end(_docs) - _left_in_leaf);
        --_left_in_leaf;
        if (target <= doc) {
          return _doc = doc;
        }
      }
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      ReadLeaf(*(std::end(_docs) - 1));
    }
  }
};

}  // namespace irs::top
