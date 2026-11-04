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

#include "iresearch/search/detail/masked_leaf.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/top/pruned_leaf.hpp"

namespace irs::top {

template<typename InputType>
class PostingPrunedClause : public PrunedProbeBase<InputType> {
  using Base = PrunedProbeBase<InputType>;

  using Base::_base;
  using Base::_cursor;
  using Base::_doc;
  using Base::_docs;
  using Base::_freqs;
  using Base::_gather;
  using Base::_lazy;
  using Base::_leaf;
  using Base::_left_in_list;
  using Base::_max_in_leaf;
  using Base::_needs_reposition;
  using Base::_provider;
  using Base::_recipe;
  using Base::DecodeLazyFreqs;
  using Base::Matches;
  using Base::Members;
  using Base::ReadFill;

 public:
  using Base::AdvanceBlock;
  using Base::MaxScore;
  using Base::Value;

  PostingPrunedClause() = default;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const SubReader& segment,
               const TermReader& field, const detail::ScoreArgs& args) {
    _leaf.Single();
    _lazy = nullptr;
    if (Base::PrepareCommon(meta, doc_in, layout, segment, field, args)) {
      _doc = doc_limits::min() + meta.doc_delta;
      _max_in_leaf = _doc;
    }
  }

  PostingPrunedClause(const PostingMeta& meta, const IndexInput& doc_in,
                      IndexFeatures layout, const SubReader& segment,
                      const TermReader& field, const detail::ScoreArgs& args) {
    Prepare(meta, doc_in, layout, segment, field, args);
  }

  ScoreFunction PrepareScore() {
    SDB_ASSERT(_recipe.segment != nullptr && _recipe.field != nullptr);
    _provider.freq.value = _gather;
    return irs::detail::PrepareScore(_recipe.args, *_recipe.segment,
                                     _recipe.field->meta(), _provider);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) noexcept {
    SDB_ASSERT(slot < kScoreBlock);
    _gather[slot] = Freq();
  }

  IRS_FORCE_INLINE ScoreMask Hits(const doc_id_t* docs, uint32_t n,
                                  uint32_t* IRS_RESTRICT freqs) {
    SDB_ASSERT(n != 0 && n <= kScoreBlock);
    const auto last = docs[n - 1];
    if (last < _doc) {
      return 0;
    }
    if (n > 2 &&
        uint64_t{last - docs[0]} * _leaf.Len() <
          uint64_t{n - 2} * irs::detail::kProbeSteps * (_max_in_leaf - _base)) {
      return Merge(docs, n, freqs);
    }
    ScoreMask hits = 0;
    for (uint32_t j = 0; j != n; ++j) {
      hits |= static_cast<ScoreMask>(Probe(docs[j]) == docs[j]) << j;
      freqs[j] = Freq();
    }
    return hits;
  }

  void ScoreWith(const ScoreFunction& score, uint32_t* freqs, score_t* scores,
                 uint32_t n) {
    _provider.freq.value = freqs;
    score.Score(scores, static_cast<scores_size_t>(n));
    _provider.freq.value = _gather;
  }

  uint32_t Keep(doc_id_t* IRS_RESTRICT docs, score_t* IRS_RESTRICT scores,
                uint32_t n) {
    uint32_t kept = 0;
    Members(docs, n,
            [&](uint32_t i, doc_id_t doc, bool hit, uint32_t freq)
              IRS_FORCE_INLINE {
                docs[kept] = doc;
                scores[kept] = scores[i];
                _gather[kept] = freq;
                kept += static_cast<uint32_t>(hit);
              });
    return kept;
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    if (target <= _max_in_leaf) [[likely]] {
      return _doc = _leaf.Find(std::begin(_docs), _base, target);
    }
    return ProbeSlow(target);
  }

 private:
  IRS_FORCE_INLINE uint32_t Freq() noexcept {
    DecodeLazyFreqs();
    return _freqs.data[_leaf.Index()];
  }

  IRS_NO_INLINE ScoreMask Merge(const doc_id_t* docs, uint32_t n,
                                uint32_t* IRS_RESTRICT freqs) {
    doc_id_t targets[kScoreBlock];
    std::fill(std::copy_n(docs, n, targets), std::end(targets),
              doc_limits::eof());
    ScoreMask hits = 0;
    Matches(targets, n, [&](uint32_t i, uint32_t freq) IRS_FORCE_INLINE {
      freqs[i] = freq;
      hits |= ScoreMask{1} << i;
    });
    return hits;
  }

  IRS_NO_INLINE doc_id_t ProbeSlow(doc_id_t target) {
    if (!_needs_reposition) [[likely]] {
      if (const auto span = _max_in_leaf - _base;
          target - _max_in_leaf <= span || target <= _cursor.UpperBound())
        [[likely]] {
        if (_left_in_list == 0) [[unlikely]] {
          return _doc = doc_limits::eof();
        }
        ReadFill(_max_in_leaf);
        if (target <= _max_in_leaf) [[likely]] {
          return _doc = _leaf.Find(std::begin(_docs), _base, target);
        }
      }
    }
    AdvanceBlock(target);
    if (_needs_reposition) {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      Base::Reposition();
      ReadFill(_doc);
    }
    while (_max_in_leaf < target) {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      ReadFill(_max_in_leaf);
    }
    return _doc = _leaf.Find(std::begin(_docs), _base, target);
  }
};

}  // namespace irs::top
