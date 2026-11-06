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

#include <limits>

#include "iresearch/search/detail/score_filter.hpp"
#include "iresearch/search/top/pruned_leaf.hpp"

namespace irs::top {

template<typename InputType>
class PostingPrunedLead : public PrunedLeafBase<InputType, false> {
  using Base = PrunedLeafBase<InputType, false>;

  using Base::_base;
  using Base::_cursor;
  using Base::_doc;
  using Base::_docs;
  using Base::_fetcher;
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
    const auto last = Base::AdvanceBlock(_doc);
    return _cursor.Armed() ? last : *(std::end(_docs) - 1);
  }

  doc_id_t SeekCompetitive(doc_id_t target, doc_id_t max, score_t threshold) {
    target = Base::SkipNonCompetitive(target, max, threshold);
    return target < max ? SkipTo(target) : target;
  }

  template<typename Visitor>
  void ForEachDocs(doc_id_t max, Visitor&& visit) {
    Base::ForEachBlock(max, [&](doc_id_t* docs, uint32_t len) IRS_FORCE_INLINE {
      visit(docs, _freqs.data + (docs - std::begin(_docs)), len);
    });
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

  template<typename Visitor>
  void ForEachCompetitiveBlock(doc_id_t max, score_t required,
                               Visitor&& visit) {
    const auto least = LeastFreq(required);
    if (least == 1) {
      Base::ForEachScoredBlock(
        max, [&](doc_id_t* docs, uint32_t len, score_t* scores)
               IRS_FORCE_INLINE { visit(docs, len, scores, len); });
      return;
    }
    Base::ForEachBlock(max, [&](doc_id_t* docs, uint32_t len) IRS_FORCE_INLINE {
      ABSL_CACHELINE_ALIGNED doc_id_t kept_docs[doc_limits::kBlockSize];
      ABSL_CACHELINE_ALIGNED uint32_t kept_freqs[doc_limits::kBlockSize];
      ABSL_CACHELINE_ALIGNED score_t kept_scores[doc_limits::kBlockSize];
      const auto kept =
        irs::detail::FilterFreqs(docs, _freqs.data + (docs - std::begin(_docs)),
                                 len, least, kept_docs, kept_freqs);
      if (kept != 0) {
        _provider.freq.value = kept_freqs;
        FetchAndScore(*_fetcher, _score, kept_docs, kept_scores, kept);
        _provider.freq.value = _freqs.data;
      }
      visit(kept_docs, kept, kept_scores, len);
    });
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
  uint32_t LeastFreq(score_t required) {
    if (required == _required) {
      return _least_freq;
    }
    _required = required;
    _least_freq = 1;
    if (!_cursor.Armed() ||
        Base::BoundScore({.freq = 1, .norm = 1}) > required) {
      return _least_freq;
    }
    auto most = _cursor.Index().Root().freq + 1;
    while (_least_freq < most) {
      const auto mid = _least_freq + (most - _least_freq) / 2;
      if (Base::BoundScore({.freq = mid, .norm = mid}) > required) {
        most = mid;
      } else {
        _least_freq = mid + 1;
      }
    }
    return _least_freq;
  }

  doc_id_t SkipTo(doc_id_t target) {
    if (!_cursor.Armed() || (_left_in_leaf != 0 && target <= _max_in_leaf)) {
      return Seek(target);
    }
    Base::AdvanceBlock(target);
    if (!_needs_reposition || _cursor.LandingDoc() + 1 != target) {
      return Seek(target);
    }
    return _left_in_list != 0 ? target : doc_limits::eof();
  }

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
    Base::AdvanceBlock(target);
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

  score_t _required = std::numeric_limits<score_t>::lowest();
  uint32_t _least_freq = 1;
};

}  // namespace irs::top
