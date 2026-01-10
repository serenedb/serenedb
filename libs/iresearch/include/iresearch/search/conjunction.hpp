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

#include <iresearch/index/iterators.hpp>

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
    : _it{std::move(it)},
      doc{irs::get<DocAttr>(*this->_it)},
      score{&ScoreAttr::get(*this->_it)} {
    SDB_ASSERT(doc);
    SDB_ASSERT(score);
  }

  ScoreAdapter(ScoreAdapter&&) noexcept = default;
  ScoreAdapter& operator=(ScoreAdapter&&) noexcept = default;

  auto* operator->() const noexcept { return _it.get(); }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept {
    return _it->GetMutable(type);
  }

  operator DocIterator::ptr&&() && noexcept { return std::move(_it); }

  explicit operator bool() const noexcept { return _it != nullptr; }

  // access iterator value without virtual call
  doc_id_t value() const noexcept { return doc->value; }

 private:
  DocIterator::ptr _it;

 public:
  const irs::DocAttr* doc{};
  const irs::ScoreAttr* score{};
};

using ScoreAdapters = std::vector<ScoreAdapter>;

// Helpers
template<typename T>
using EmptyWrapper = T;

struct SubScores {
  std::vector<irs::ScoreAttr*> scores;
  score_t sum_score = 0.f;
};

struct ConjunctionScoreContext {
  void Reset(auto& iterators) {
    scorers.reserve(iterators.size());
    for (auto& it : iterators) {
      if (!it.score || it.score->IsDefault()) {
        continue;
      }
      scorers.emplace_back(it.score);
    }
  }

  bool Empty() const noexcept { return scorers.empty(); }

  template<ScoreMergeType MergeType>
  void Score(score_t* res) noexcept {
    SDB_ASSERT(!scorers.empty());
    scores.resize(size);

    const size_t count = scorers.size();

    scorers[0]->Score(res);
    for (size_t i = 1; i < count; ++i) {
      scorers[i]->Score(scores.data());
      Merge<MergeType>(res, std::span{scores});
    }

    size = 0;
  }

  void Collect() {
    for (auto* scorer : scorers) {
      scorer->Collect();
    }
    ++size;
  }

  std::vector<const ScoreFunction*> scorers;
  absl::InlinedVector<score_t, kScoreWindow> scores;
  uint32_t size = 0;
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
template<typename Adapter, ScoreMergeType MergeType>
struct ConjunctionBase : public DocIterator, public ScoreCtx {
 public:
  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

 protected:
  static_assert(std::is_base_of_v<ScoreAdapter, Adapter>);

  explicit ConjunctionBase(std::vector<Adapter>&& itrs)
    : _itrs{std::move(itrs)} {
    SDB_ASSERT(
      absl::c_is_sorted(_itrs, [](const auto& lhs, const auto& rhs) noexcept {
        return CostAttr::extract(lhs, CostAttr::kMax) <
               CostAttr::extract(rhs, CostAttr::kMax);
      }));
  }

  void PrepareScore(irs::ScoreAttr& score, auto min) {
    if constexpr (kHasScore) {
      _scores.Reset(_itrs);

      if (_scores.Empty()) {
        score = ScoreFunction::Default();
        return;
      }

      score = {*this,
               [](ScoreCtx* ctx, score_t* res) noexcept {
                 auto& self = *static_cast<ConjunctionBase*>(ctx);
                 self._scores.template Score<MergeType>(res);
               },
               [](ScoreCtx* ctx) noexcept {
                 auto& self = *static_cast<ConjunctionBase*>(ctx);
                 self._scores.Collect();
               },
               min};
    }
  }

  auto begin() const noexcept { return _itrs.begin(); }
  auto end() const noexcept { return _itrs.end(); }
  size_t size() const noexcept { return _itrs.size(); }

  std::vector<Adapter> _itrs;
  [[no_unique_address]] utils::Need<kHasScore, ConjunctionScoreContext> _scores;
};

template<typename Adapter, ScoreMergeType MergeType>
class Conjunction : public ConjunctionBase<Adapter, MergeType> {
  using Base = ConjunctionBase<Adapter, MergeType>;
  using Attributes =
    std::tuple<AttributePtr<DocAttr>, AttributePtr<CostAttr>, ScoreAttr>;

 public:
  explicit Conjunction(std::vector<Adapter>&& itrs)
    : Base{std::move(itrs)}, _front{this->_itrs.front()} {
    SDB_ASSERT(!this->_itrs.empty());
    SDB_ASSERT(_front);

    std::get<AttributePtr<DocAttr>>(_attrs) = irs::GetMutable<DocAttr>(&_front);
    std::get<AttributePtr<CostAttr>>(_attrs) =
      irs::GetMutable<CostAttr>(&_front);

    this->PrepareScore(std::get<ScoreAttr>(_attrs), ScoreFunction::NoopMin);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const final {
    return std::get<AttributePtr<DocAttr>>(_attrs).ptr->value;
  }

  doc_id_t advance() final { return converge(_front->advance()); }

  doc_id_t seek(doc_id_t target) final {
    return converge(_front->seek(target));
  }

  uint32_t count() final { return DocIterator::Count(*this); }

  uint32_t collect(std::span<doc_id_t> docs) final {
    return DocIterator::Collect(*this, docs, [&] {
      if constexpr (Base::kHasScore) {
        this->_scores.Collect();
      }
    });
  }

 private:
  // tries to converge front_ and other iterators to the specified target.
  // if it impossible tries to find first convergence place
  doc_id_t converge(doc_id_t target) {
    if (doc_limits::eof(target)) [[unlikely]] {
      return doc_limits::eof();
    }
    const auto begin = this->_itrs.begin() + 1;
    const auto end = this->_itrs.end();
  restart:
    SDB_ASSERT(!doc_limits::eof(target));
    for (auto it = begin; it != end; ++it) {
      const auto doc = (*it)->seek(target);
      if (target < doc) {
        target = _front->seek(doc);
        if (!doc_limits::eof(target)) [[likely]] {
          goto restart;
        }
        return target;
      }
    }
    return target;
  }

  Attributes _attrs;
  Adapter& _front;
};

template<bool Root, typename Adapter, ScoreMergeType MergeType>
class BlockConjunction : public ConjunctionBase<Adapter, MergeType> {
  using Base = ConjunctionBase<Adapter, MergeType>;
  using Attributes =
    std::tuple<DocAttr, AttributePtr<CostAttr>, irs::ScoreAttr>;

 public:
  using Adapters = std::vector<Adapter>;

  explicit BlockConjunction(Adapters&& itrs, SubScores&& scores, bool strict)
    : Base{std::move(itrs)}, _sum_scores{scores.sum_score} {
    SDB_ASSERT(this->_itrs.size() >= 2);
    // SDB_ASSERT(!this->_scores.empty());
    // absl::c_sort(this->_scores, [](const auto* lhs, const auto* rhs) {
    //   return lhs->max.tail > rhs->max.tail;
    // });
    std::get<AttributePtr<CostAttr>>(_attrs) =
      irs::GetMutable<CostAttr>(&this->_itrs.front());
    auto& score = std::get<ScoreAttr>(_attrs);
    score.max.leaf = score.max.tail = _sum_scores;
    auto min = strict ? MinStrictN : MinWeakN;
    // if constexpr (Root) {
    //   auto score_root = [](ScoreCtx* ctx, score_t* res) noexcept {
    //     auto& self = static_cast<BlockConjunction&>(*ctx);
    //     std::memcpy(res, self._score.temp(),
    //                 static_cast<Merger&>(self).byte_size());
    //   };
    //   this->PrepareScore(score, score_root, score_root, min);
    // } else {
    //   this->PrepareScore(score, min);
    // }
    this->PrepareScore(score, min);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  IRS_FORCE_INLINE auto& score() { return std::get<ScoreAttr>(_attrs).max; }

  doc_id_t value() const final { return std::get<DocAttr>(_attrs).value; }

  doc_id_t advance() final { return seek(value() + 1); }

  doc_id_t seek(doc_id_t target) final {
    auto& doc = std::get<DocAttr>(_attrs).value;
    if (target <= doc) [[unlikely]] {
      if constexpr (Root) {
        // if (_threshold < _score.temp()[0]) {
        //   return doc;
        // }
        target = doc + !doc_limits::eof(doc);
      } else {
        return doc;
      }
    }
  align_leafs:
    target = ShallowSeekImpl(target);
  align_docs:
    if (doc_limits::eof(target)) [[unlikely]] {
      return Seal();
    }
    auto it = this->_itrs.begin();
    const auto seek_target = (*it)->seek(target);
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
      target = (*it)->seek(seek_target);
      if (target != seek_target) {
        if (target > _leafs_doc) {
          goto align_leafs;
        }
        goto align_docs;
      }
      ++it;
    } while (it != end);
    doc = seek_target;

    // if constexpr (Root) {
    // auto& merger = static_cast<Merger&>(*this);
    //   auto begin = this->_scores.begin();
    //   auto end = this->_scores.end();

    //  (**begin)(_score.temp());
    //  for (++begin; begin != end; ++begin) {
    //    (**begin)(merger.temp());
    //    merger(_score.temp(), merger.temp());
    //  }
    //  if (_threshold < _score.temp()[0]) {
    //    return target;
    //  }
    //  ++target;
    //  if (target > _leafs_doc) {
    //    goto align_leafs;
    //  }
    //  goto align_docs;
    //} else {
    return target;
    //}
  }

  doc_id_t shallow_seek(doc_id_t target) final {
    target = ShallowSeekImpl(target);
    if (doc_limits::eof(target)) [[unlikely]] {
      return Seal();
    }
    return _leafs_doc;
  }

  uint32_t count() final { return DocIterator::Count(*this); }

 private:
  // TODO(mbkkt) Maybe optimize for 2?
  static void MinN(BlockConjunction& self, score_t arg) noexcept {
    // for (auto* score : self._scores) {
    //   const auto others = self._sum_scores - score->max.tail;
    //   if (arg <= others) {
    //     return;
    //   }
    //   score->Min(arg - others);
    // }
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

    // auto& merger = static_cast<Merger&>(*this);
    for (auto& it : this->_itrs) {
      auto max_leaf = it->shallow_seek(target);
      auto min_leaf = it.doc->value;
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
      // merger.Merge(sum_leafs_score, it.score->max.leaf);
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
  // typename Merger::Buffer _score;
};

// Returns conjunction iterator created from the specified sub iterators
template<ScoreMergeType MergeType,
         template<typename> typename Wrapper = EmptyWrapper, typename Adapter,
         typename... Args>
DocIterator::ptr MakeConjunction(WandContext ctx, std::vector<Adapter>&& itrs,
                                 Args&&... args) {
  if (const auto size = itrs.size(); 0 == size) {
    // empty or unreachable search criteria
    return DocIterator::empty();
  } else if (1 == size) {
    // single sub-query
    return std::move(itrs.front());
  }

  // conjunction
  absl::c_sort(itrs, [](const auto& lhs, const auto& rhs) noexcept {
    return CostAttr::extract(lhs, CostAttr::kMax) <
           CostAttr::extract(rhs, CostAttr::kMax);
  });
  SubScores scores;
  using ConjunctionImpl = Conjunction<Adapter, MergeType>;
  using WrappedConjunction = Wrapper<ConjunctionImpl>;
  if constexpr (false && MergeType != ScoreMergeType::Noop &&
                std::is_same_v<ConjunctionImpl, WrappedConjunction>) {
    scores.scores.reserve(itrs.size());
    // TODO(mbkkt) Find better one
    static constexpr size_t kBlockConjunctionCostThreshold = 1;
    bool use_block =
      ctx.Enabled() && CostAttr::extract(itrs.front(), CostAttr::kMax) >
                         kBlockConjunctionCostThreshold;
    for (auto& it : itrs) {
      // FIXME(gnusi): remove const cast
      auto* score = const_cast<irs::ScoreAttr*>(it.score);
      SDB_ASSERT(score);  // ensured by ScoreAdapter
      if (score->IsDefault()) {
        continue;
      }
      scores.scores.emplace_back(score);
      const auto tail = score->max.tail;
      use_block &= tail != std::numeric_limits<score_t>::max();
      if (use_block) {
        scores.sum_score += tail;
      }
    }
    use_block &= !scores.scores.empty();
    if (use_block) {
      return ResolveBool(ctx.root, [&]<bool Root> -> DocIterator::ptr {
        return memory::make_managed<
          Wrapper<BlockConjunction<Root, Adapter, MergeType>>>(
          std::forward<Args>(args)..., std::move(itrs), std::move(scores),
          ctx.strict);
      });
    }
    // TODO(mbkkt) We still could set min producer and root scoring
  }

  return memory::make_managed<Wrapper<Conjunction<Adapter, MergeType>>>(
    std::forward<Args>(args)...,
    std::move(itrs) /*, std::move(scores.scores)*/);
}

}  // namespace irs
