////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/inlined_vector.h>

#include <cstddef>
#include <cstdint>
#include <iresearch/index/iterators.hpp>
#include <ranges>

#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/type_limits.hpp"

// Conjunction is template for Adapter instead of direct use of ScoreAdapter
// only because of ngram
namespace irs {

// Adapter to use DocIterator::ptr with conjunction and disjunction.
struct ScoreAdapter {
  ScoreAdapter() = default;

  ScoreAdapter(DocIterator::ptr it) noexcept
    : _it{std::move(it)}, _score{&ScoreAttr::get(*this->_it)} {
    const auto* doc = irs::get<DocAttr>(*this->_it);
    SDB_ASSERT(doc);
    _doc = &doc->value;
  }

  ScoreAdapter(ScoreAdapter&&) noexcept = default;
  ScoreAdapter& operator=(ScoreAdapter&&) noexcept = default;

  IRS_FORCE_INLINE operator DocIterator::ptr&&() && noexcept {
    return std::move(_it);
  }

  IRS_FORCE_INLINE explicit operator bool() const noexcept {
    return _it != nullptr;
  }

  IRS_FORCE_INLINE Attribute* GetMutable(TypeInfo::type_id type) noexcept {
    return _it->GetMutable(type);
  }

  IRS_FORCE_INLINE doc_id_t value() const noexcept { return *_doc; }

  IRS_FORCE_INLINE doc_id_t advance() { return _it->advance(); }

  IRS_FORCE_INLINE doc_id_t seek(doc_id_t target) { return _it->seek(target); }

  IRS_FORCE_INLINE doc_id_t shallow_seek(doc_id_t target) {
    return _it->shallow_seek(target);
  }

  IRS_FORCE_INLINE uint32_t count() { return _it->count(); }

  IRS_FORCE_INLINE uint32_t collect(std::span<doc_id_t> docs) {
    return _it->collect(docs);
  }

  IRS_FORCE_INLINE void CollectData(uint16_t index) {
    return _it->CollectData(index);
  }

  IRS_FORCE_INLINE const ScoreAttr& score() const noexcept { return *_score; }

  IRS_FORCE_INLINE const ScoreFunction& PrepareScore(
    const PrepareScoreContext& ctx) const noexcept {
    return _it->PrepareScore(ctx);
  }

  IRS_FORCE_INLINE std::pair<doc_id_t, bool> CollectBlock(
    doc_id_t min, doc_id_t max, ScoreMergeType merge_type,
    const ScoreFunction* score, uint64_t* IRS_RESTRICT mask,
    score_t* IRS_RESTRICT scores, uint32_t* IRS_RESTRICT matches,
    size_t min_match_count) {
    return _it->CollectBlock(min, max, merge_type, score, mask, scores, matches,
                             min_match_count);
  }

 private:
  DocIterator::ptr _it;
  const doc_id_t* _doc{};
  const ScoreAttr* _score{};
};

using ScoreAdapters = std::vector<ScoreAdapter>;

template<typename T>
using EmptyWrapper = T;

struct SubScores {
  std::vector<const ScoreAttr*> scores;
  score_t sum_score = 0.f;
};

auto ToScores(const PrepareScoreContext& ctx, auto& itrs) noexcept {
  return itrs |
         std::views::filter([](auto& it) { return !it.score().IsDefault(); }) |
         std::views::transform([&](auto& it) { return &it.PrepareScore(ctx); });
}

class ConjunctionScorer : public ScoreCtx {
 public:
  static ScoreFunction Make(ScoreMergeType merge_type,
                            const PrepareScoreContext& ctx, auto& itrs,
                            auto min) {
    if (merge_type == ScoreMergeType::Noop) {
      return ScoreFunction::Default();
    }

    std::vector<const ScoreFunction*> sources;
    sources.reserve(itrs.size());
    sources.append_range(ToScores(ctx, itrs));

    if (sources.empty()) {
      return ScoreFunction::Default();
    }

    return ResolveMergeType(merge_type, [&]<ScoreMergeType MergeType> {
      return ScoreFunction::Make<ConjunctionScorer>(
        [](ScoreCtx* ctx, score_t* res, size_t n) noexcept {
          auto& self = *static_cast<ConjunctionScorer*>(ctx);
          auto source = self._sources.begin();
          auto end = self._sources.end();

          (*source)->Score(res, n);
          for (++source; source != end; ++source) {
            (*source)->Score(self._scores.data(), n);
            Merge<MergeType>(res, self._scores.data(), n);
          }
        },
        min, std::move(sources));
    });
  }

  explicit ConjunctionScorer(std::vector<const ScoreFunction*>&& sources)
    : _sources{std::move(sources)} {}

 private:
  std::vector<const ScoreFunction*> _sources;
  std::array<score_t, kScoreBlock> _scores;
};

// Conjunction of N iterators
// -----------------------------------------------------------------------------
// c |  [0] <-- lead (the least cost iterator)
// o |  [1]    |
// s |  [2]    | tail (other iterators)
// t |  ...    |
//   V  [n] <-- end
// -----------------------------------------------------------------------------
// goto used instead of labeled cycles, with them we can achieve best perfomance
template<typename Adapter>
struct ConjunctionBase : public DocIterator {
 public:
  void CollectData(uint16_t index) final {
    for (auto& it : _itrs) {
      it.CollectData(index);
    }
  }

 protected:
  explicit ConjunctionBase(ScoreMergeType merge_type,
                           std::vector<Adapter>&& itrs)
    : _merge_type{merge_type}, _itrs{std::move(itrs)} {
    SDB_ASSERT(
      absl::c_is_sorted(_itrs, [](const auto& lhs, const auto& rhs) noexcept {
        return CostAttr::extract(lhs, CostAttr::kMax) <
               CostAttr::extract(rhs, CostAttr::kMax);
      }));
  }

  auto begin() const noexcept { return _itrs.begin(); }
  auto end() const noexcept { return _itrs.end(); }
  size_t size() const noexcept { return _itrs.size(); }

  ScoreMergeType _merge_type;
  std::vector<Adapter> _itrs;
};

template<typename Adapter>
class Conjunction : public ConjunctionBase<Adapter> {
  using Base = ConjunctionBase<Adapter>;
  using Attributes =
    std::tuple<AttributePtr<DocAttr>, AttributePtr<CostAttr>, ScoreAttr>;

 public:
  explicit Conjunction(ScoreMergeType merge_type, std::vector<Adapter>&& itrs)
    : Base{merge_type, std::move(itrs)} {
    SDB_ASSERT(!this->_itrs.empty());

    std::get<AttributePtr<DocAttr>>(_attrs) =
      irs::GetMutable<DocAttr>(&this->_itrs[0]);
    std::get<AttributePtr<CostAttr>>(_attrs) =
      irs::GetMutable<CostAttr>(&this->_itrs[0]);
  }

  const ScoreFunction& PrepareScore(const PrepareScoreContext& ctx) final {
    auto& score = std::get<irs::ScoreAttr>(_attrs);
    score = ConjunctionScorer::Make(this->_merge_type, ctx, this->_itrs,
                                    ScoreFunction::NoopMin);
    return score;
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  IRS_FORCE_INLINE doc_id_t value() const noexcept final {
    return std::get<AttributePtr<DocAttr>>(_attrs).ptr->value;
  }

  doc_id_t advance() final { return converge(this->_itrs[0].advance()); }

  doc_id_t seek(doc_id_t target) final {
    return converge(this->_itrs[0].seek(target));
  }

  uint32_t count() final { return DocIterator::Count(*this); }

  uint32_t collect(std::span<doc_id_t> docs) final {
    return DocIterator::Collect(*this, docs);
  }

 private:
  // tries to converge front_ and other iterators to the specified target.
  // if it impossible tries to find first convergence place
  doc_id_t converge(doc_id_t target) {
    const auto begin = this->_itrs.begin() + 1;
    const auto end = this->_itrs.end();
  restart:
    if (doc_limits::eof(target)) [[unlikely]] {
      return doc_limits::eof();
    }
    for (auto it = begin; it != end; ++it) {
      const auto doc = it->seek(target);
      if (target < doc) {
        target = this->_itrs[0].seek(doc);
        goto restart;
      }
    }
    return target;
  }

  Attributes _attrs;
};

template<typename Adapter, ScoreMergeType MergeType>
class BlockConjunction : public ConjunctionBase<Adapter> {
  using Base = ConjunctionBase<Adapter>;
  using Attributes = std::tuple<DocAttr, AttributePtr<CostAttr>, ScoreAttr>;

 public:
  using Adapters = std::vector<Adapter>;

  explicit BlockConjunction(Adapters&& itrs, SubScores&& scores, bool strict)
    : Base{MergeType, std::move(itrs)},
      _sum_scores{scores.sum_score},
      _strict{strict} {
    SDB_ASSERT(this->_itrs.size() >= 2);
    SDB_ASSERT(!this->_scores.scorers.empty());
    // absl::c_sort(this->_scores, [](const auto* lhs, const auto* rhs) {
    //   return lhs->max.tail > rhs->max.tail;
    // });
    std::get<AttributePtr<CostAttr>>(_attrs) =
      irs::GetMutable<CostAttr>(&this->_itrs[0]);
  }

  const ScoreFunction& PrepareScore(const PrepareScoreContext& ctx) final {
    auto& score = std::get<irs::ScoreAttr>(_attrs);
    score.max.leaf = score.max.tail = _sum_scores;
    score = ConjunctionScorer::Make(this->_merge_type, ctx, this->_itrs,
                                    _strict ? MinStrictN : MinWeakN);
    return score;
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  doc_id_t advance() final { return seek(value() + 1); }

  doc_id_t seek(doc_id_t target) final {
    auto& doc = std::get<DocAttr>(_attrs).value;
    if (target <= doc) [[unlikely]] {
      return doc;
    }
  align_leafs:
    target = ShallowSeekImpl(target);
  align_docs:
    if (doc_limits::eof(target)) [[unlikely]] {
      return Seal();
    }
    auto it = this->_itrs.begin();
    const auto seek_target = it->seek(target);
    if (seek_target > _leafs_doc) {
      target = seek_target;
      goto align_leafs;
    }
    if (doc_limits::eof(seek_target)) [[unlikely]] {
      return Seal();
    }
    ++it;
    const auto end = this->_itrs.end();
    do {
      target = it->seek(seek_target);
      if (target != seek_target) {
        if (target > _leafs_doc) {
          goto align_leafs;
        }
        goto align_docs;
      }
      ++it;
    } while (it != end);
    doc = seek_target;

    return target;
  }

  doc_id_t shallow_seek(doc_id_t target) final {
    target = ShallowSeekImpl(target);
    if (doc_limits::eof(target)) [[unlikely]] {
      return Seal();
    }
    return _leafs_doc;
  }

  uint32_t count() final { return DocIterator::Count(*this); }

  uint32_t collect(std::span<doc_id_t> docs) final {
    return DocIterator::Collect(*this, docs);
  }

 private:
  IRS_FORCE_INLINE auto& score() { return std::get<ScoreAttr>(_attrs).max; }

  // TODO(mbkkt) Maybe optimize for 2?
  static void MinN(BlockConjunction& self, score_t arg) noexcept {
    for (auto* score : self._scores.scorers) {
      const auto others = self._sum_scores - score->max.tail;
      if (arg <= others) {
        return;
      }
      score->Min(arg - others);
    }
  }

  static void MinStrictN(ScoreCtx* ctx, score_t arg) noexcept {
    auto& self = static_cast<BlockConjunction&>(*ctx);
    SDB_ASSERT(self._threshold <= arg);
    self._threshold = arg;
    MinN(self, arg);
  }

  static void MinWeakN(ScoreCtx* ctx, score_t arg) noexcept {
    auto& self = static_cast<BlockConjunction&>(*ctx);
    SDB_ASSERT(self._threshold <= arg);
    self._threshold = std::nextafter(arg, 0.f);
    MinN(self, arg);
  }

  IRS_NO_INLINE doc_id_t Seal() {
    _leafs_doc = doc_limits::eof();
    std::get<DocAttr>(_attrs).value = doc_limits::eof();
    score().leaf = {};
    score().tail = {};
    return doc_limits::eof();
  }

  doc_id_t ShallowSeekImpl(doc_id_t target) {
    auto& doc = std::get<DocAttr>(_attrs).value;
    if (target <= _leafs_doc) {
    score_check:
      if (_threshold < score().leaf) {
        return target;
      }
      target = _leafs_doc + !doc_limits::eof(_leafs_doc);
    }
    SDB_ASSERT(doc <= _leafs_doc);
  eof_check:
    if (doc_limits::eof(target)) [[unlikely]] {
      return target;
    }

    auto max_leafs = doc_limits::eof();
    auto min_leafs = doc;
    score_t sum_leafs_score = 0.f;

    for (auto& it : this->_itrs) {
      auto max_leaf = it.shallow_seek(target);
      auto min_leaf = it.value();
      SDB_ASSERT(min_leaf <= max_leaf);
      if (target < min_leaf) {
        target = min_leaf;
      }
      if (max_leafs < min_leaf) {
        goto eof_check;
      }
      if (min_leafs < min_leaf) {
        min_leafs = min_leaf;
      }
      if (max_leafs > max_leaf) {
        max_leafs = max_leaf;
      }
      SDB_ASSERT(min_leafs <= max_leafs);
      Merge<MergeType>(sum_leafs_score, it.score().max.leaf);
    }

    _leafs_doc = max_leafs;
    doc = min_leafs;
    score().leaf = sum_leafs_score;
    SDB_ASSERT(doc <= target);
    SDB_ASSERT(target <= _leafs_doc);
    goto score_check;
  }

  Attributes _attrs;
  score_t _sum_scores;
  doc_id_t _leafs_doc{doc_limits::invalid()};
  score_t _threshold{};
  bool _strict;
};

// Returns conjunction iterator created from the specified sub iterators
template<template<typename> typename Wrapper = EmptyWrapper, typename Adapter,
         typename... Args>
DocIterator::ptr MakeConjunction(ScoreMergeType merge_type, WandContext ctx,
                                 std::vector<Adapter>&& itrs, Args&&... args) {
  if (const auto size = itrs.size(); 0 == size) {
    // empty or unreachable search criteria
    return DocIterator::empty();
  } else if (1 == size) {
    // single sub-query
    return std::move(itrs[0]);
  }

  // conjunction
  absl::c_sort(itrs, [](const auto& lhs, const auto& rhs) noexcept {
    return CostAttr::extract(lhs, CostAttr::kMax) <
           CostAttr::extract(rhs, CostAttr::kMax);
  });
  SubScores scores;
  using ConjunctionImpl = Conjunction<Adapter>;
  using WrappedConjunction = Wrapper<ConjunctionImpl>;
  if constexpr (false && merge_type != ScoreMergeType::Noop &&
                std::is_same_v<ConjunctionImpl, WrappedConjunction>) {
    scores.scores.reserve(itrs.size());
    // TODO(mbkkt) Find better one
    static constexpr size_t kBlockConjunctionCostThreshold = 1;
    bool use_block =
      ctx.Enabled() && CostAttr::extract(itrs[0], CostAttr::kMax) >
                         kBlockConjunctionCostThreshold;
    for (auto& it : itrs) {
      const auto& score = it.score();
      if (score.IsDefault()) {
        continue;
      }
      scores.scores.emplace_back(&score);
      const auto tail = score.max.tail;
      use_block &= tail != std::numeric_limits<score_t>::max();
      if (use_block) {
        scores.sum_score += tail;
      }
    }
    use_block &= !scores.scores.empty();
    if (use_block) {
      return ResolveMergeType(merge_type, [&]<ScoreMergeType MergeType> {
        auto it = memory::make_managed<BlockConjunction<Adapter, MergeType>>(
          std::forward<Args>(args)..., std::move(itrs), std::move(scores),
          ctx.strict);
        return it;
      });
    }
    // TODO(mbkkt) We still could set min producer and root scoring
  }

  auto it = memory::make_managed<Wrapper<Conjunction<Adapter>>>(
    merge_type, std::forward<Args>(args)...,
    std::move(itrs) /*, std::move(scores.scores)*/);
  return it;
}

}  // namespace irs
