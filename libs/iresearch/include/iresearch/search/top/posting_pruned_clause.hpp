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

#include "iresearch/search/top/detail/prune_leaf.hpp"

namespace irs::search {

template<typename InputType>
class PostingPrunedClause : public PruneLeafBase<InputType, false> {
  using Base = PruneLeafBase<InputType, false>;

  using Base::_base;
  using Base::_doc;
  using Base::_docs;
  using Base::_freqs;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::_len;
  using Base::_max_in_leaf;
  using Base::_needs_reposition;
  using Base::_provider;
  using Base::_recipe;
  using Base::_skip;
  using Base::_upper_bound;
  using Base::ReadLeaf;

 public:
  using Base::MaxScore;
  using Base::Value;

  PostingPrunedClause() = default;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const SubReader& segment,
               const TermReader& field, const ScoreArgs& args) {
    if (Base::PrepareCommon(meta, doc_in, layout, segment, field, args)) {
      _left_in_leaf = 0;
      _doc = doc_limits::min() + meta.doc_delta;
    }
  }

  PostingPrunedClause(const PostingMeta& meta, const IndexInput& doc_in,
                      IndexFeatures layout, const SubReader& segment,
                      const TermReader& field, const ScoreArgs& args) {
    Prepare(meta, doc_in, layout, segment, field, args);
  }

  doc_id_t AdvanceBlock(doc_id_t target) {
    if (_skip.NumLevels() == 0) [[unlikely]] {
      return doc_limits::eof();
    }
    auto& reader = _skip.Reader();
    const auto upper_bound = reader.UpperBound();
    if (upper_bound >= target) {
      return upper_bound;
    }
    const auto below = reader.SkipBoundsBelow();
    reader.SetSkipBoundsBelow(std::max(below, target));
    _left_in_list = _skip.Seek(target);
    reader.SetSkipBoundsBelow(below);
    _left_in_leaf = 0;
    _needs_reposition = true;
    _doc = reader.State().doc;
    _upper_bound = reader.UpperBound();
    return _upper_bound;
  }

  ScoreFunction PrepareScore() {
    SDB_ASSERT(_recipe.segment != nullptr && _recipe.field != nullptr);
    SDB_ASSERT(_recipe.args.scorer != nullptr);
    _provider.freq.value = _gather;
    return _recipe.args.scorer->PrepareScorer({
      .segment = *_recipe.segment,
      .field = _recipe.field->meta(),
      .doc_attrs = _provider,
      .fetcher = _recipe.args.fetcher,
      .stats = _recipe.args.stats,
      .boost = _recipe.args.boost,
    });
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) noexcept {
    SDB_ASSERT(slot < kScoreBlock);
    _gather[slot] = _freqs.data[doc_limits::kBlockSize - _left_in_leaf - 1];
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    if (_left_in_leaf != 0 && target <= _max_in_leaf) [[likely]] {
      return _doc = TakeFrom(target);
    }
    return ProbeSlow(target);
  }

 private:
  IRS_FORCE_INLINE doc_id_t TakeFrom(doc_id_t target) noexcept {
    if (_len == doc_limits::kBlockSize) [[likely]] {
      const auto* const it =
        BranchlessLowerBound<doc_limits::kBlockSize>(std::begin(_docs), target);
      _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - it) - 1;
      return *it;
    }
    for (;;) {
      const auto doc = *(std::end(_docs) - _left_in_leaf);
      --_left_in_leaf;
      if (target <= doc) {
        return doc;
      }
    }
  }

  doc_id_t SeekToBlock(doc_id_t target) {
    if (_skip.NumLevels() == 0) [[unlikely]] {
      return doc_limits::eof();
    }
    auto& reader = _skip.Reader();
    const auto upper_bound = reader.UpperBound();
    if (upper_bound >= target) {
      return upper_bound;
    }
    const auto below = reader.SkipBoundsBelow();
    reader.SetSkipBoundsBelow(std::max(below, target));
    _left_in_list = _skip.Seek(target);
    reader.SetSkipBoundsBelow(below);
    _left_in_leaf = 0;
    _needs_reposition = true;
    _upper_bound = reader.UpperBound();
    return _upper_bound;
  }

  IRS_NO_INLINE doc_id_t ProbeSlow(doc_id_t target) {
    if (!_needs_reposition) [[likely]] {
      if (const auto span = _max_in_leaf - _base;
          target - _max_in_leaf <= span || target <= _upper_bound) [[likely]] {
        if (_left_in_list == 0) [[unlikely]] {
          _left_in_leaf = 0;
          return _doc = doc_limits::eof();
        }
        ReadLeaf(_max_in_leaf);
        if (target <= _max_in_leaf) [[likely]] {
          return _doc = TakeFrom(target);
        }
      }
    }
    if (_skip.Reader().IsLessThanUpperBound(target)) [[unlikely]] {
      if (!doc_limits::eof(SeekToBlock(target))) {
        _doc = _skip.Reader().State().doc;
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

  ABSL_CACHELINE_ALIGNED uint32_t _gather[kScoreBlock]{};
};

}  // namespace irs::search
