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
  using Base::DecodeFreqs;
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
    SDB_ASSERT(_recipe.args.scorer != nullptr);
    _provider.freq.value = _gather;
    return _recipe.args.scorer->PrepareScorer({
      .segment = *_recipe.segment,
      .field = _recipe.field->meta(),
      .doc_attrs = _provider,
      .fetcher = *_recipe.args.fetcher,
      .stats = _recipe.args.stats,
      .boost = _recipe.args.boost,
    });
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) noexcept {
    SDB_ASSERT(slot < kScoreBlock);
    if constexpr (InputType::kVolatileAlways) {
      if (_lazy != nullptr) {
        DecodeFreqs();
      }
    }
    _gather[slot] = _freqs.data[_leaf.Index()];
  }

  uint32_t Keep(doc_id_t* IRS_RESTRICT docs, score_t* IRS_RESTRICT scores,
                uint32_t n) {
    uint32_t kept = 0;
    Members(docs, n, [&](uint32_t i, bool hit, uint32_t freq) IRS_FORCE_INLINE {
      docs[kept] = docs[i];
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
