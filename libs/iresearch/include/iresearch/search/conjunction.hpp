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

#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

// Adapter to use DocIteratorImpl with conjunction and disjunction.
template<typename DocIteratorImpl = DocIterator::ptr>
struct ScoreAdapter {
  ScoreAdapter() noexcept = default;
  ScoreAdapter(DocIteratorImpl&& it) noexcept
    : it{std::move(it)},
      doc{irs::get<irs::DocAttr>(*this->it)},
      score{&irs::ScoreAttr::get(*this->it)} {
    SDB_ASSERT(doc);
  }

  ScoreAdapter(ScoreAdapter&&) noexcept = default;
  ScoreAdapter& operator=(ScoreAdapter&&) noexcept = default;

  auto* operator->() const noexcept { return it.get(); }

  const Attribute* get(TypeInfo::type_id type) const noexcept {
    return it->get(type);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept {
    return it->GetMutable(type);
  }

  operator DocIteratorImpl&&() && noexcept { return std::move(it); }

  explicit operator bool() const noexcept { return it != nullptr; }

  // access iterator value without virtual call
  doc_id_t value() const noexcept { return doc->value; }

  DocIteratorImpl it;
  const irs::DocAttr* doc{};
  const irs::ScoreAttr* score{};
};

using ScoreAdapters = std::vector<ScoreAdapter<>>;

// Helpers
template<typename T>
using EmptyWrapper = T;

struct SubScores {
  std::vector<irs::ScoreAttr*> scores;
  score_t sum_score = 0.f;
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
template<typename DocIteratorImpl, typename Merger>
struct ConjunctionBase : public DocIterator,
                         protected Merger,
                         protected ScoreCtx {
 protected:
  explicit ConjunctionBase(Merger&& merger, std::vector<DocIteratorImpl>&& itrs,
                           std::vector<irs::ScoreAttr*>&& scorers)
    : Merger{std::move(merger)},
      _itrs{std::move(itrs)},
      _scores{std::move(scorers)} {
    SDB_ASSERT(
      absl::c_is_sorted(_itrs, [](const auto& lhs, const auto& rhs) noexcept {
        return CostAttr::extract(lhs, CostAttr::kMax) <
               CostAttr::extract(rhs, CostAttr::kMax);
      }));
  }

  static void Score2(ScoreCtx* ctx, score_t* res) noexcept {
    auto& self = static_cast<ConjunctionBase&>(*ctx);
    auto& merger = static_cast<Merger&>(self);
    (*self._scores.front())(res);
    (*self._scores.back())(merger.temp());
    merger(res, merger.temp());
  }

  static void ScoreN(ScoreCtx* ctx, score_t* res) noexcept {
    auto& self = static_cast<ConjunctionBase&>(*ctx);
    auto& merger = static_cast<Merger&>(self);
    auto it = self._scores.begin();
    auto end = self._scores.end();
    (**it)(res);
    ++it;
    do {
      (**it)(merger.temp());
      merger(res, merger.temp());
      ++it;
    } while (it != end);
  }

  void PrepareScore(irs::ScoreAttr& score, auto score_2, auto score_n,
                    auto min) {
    SDB_ASSERT(Merger::size());
    switch (_scores.size()) {
      case 0:
        score = ScoreFunction::Default(Merger::size());
        break;
      case 1:
        score = std::move(*_scores.front());
        break;
      case 2:
        score.Reset(*this, score_2, min);
        break;
      default:
        score.Reset(*this, score_n, min);
        break;
    }
  }

  auto begin() const noexcept { return _itrs.begin(); }
  auto end() const noexcept { return _itrs.end(); }
  size_t size() const noexcept { return _itrs.size(); }

  std::vector<DocIteratorImpl> _itrs;
  std::vector<ScoreAttr*> _scores;
};

template<typename DocIteratorImpl, typename Merger>
class Conjunction : public ConjunctionBase<DocIteratorImpl, Merger> {
  using Base = ConjunctionBase<DocIteratorImpl, Merger>;
  using Attributes =
    std::tuple<AttributePtr<DocAttr>, AttributePtr<CostAttr>, ScoreAttr>;

 public:
  explicit Conjunction(Merger&& merger, std::vector<DocIteratorImpl>&& itrs,
                       std::vector<irs::ScoreAttr*>&& scores = {})
    : Base{std::move(merger), std::move(itrs), std::move(scores)},
      _front{this->_itrs.front().it.get()} {
    SDB_ASSERT(!this->_itrs.empty());
    SDB_ASSERT(_front);

    auto* front_doc = irs::GetMutable<DocAttr>(_front);
    _front_doc = &front_doc->value;
    std::get<AttributePtr<DocAttr>>(_attrs) = front_doc;

    std::get<AttributePtr<CostAttr>>(_attrs) =
      irs::GetMutable<CostAttr>(_front);

    if constexpr (kHasScore<Merger>) {
      auto& score = std::get<irs::ScoreAttr>(_attrs);
      this->PrepareScore(score, Base::Score2, Base::ScoreN,
                         ScoreFunction::DefaultMin);
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const final { return *_front_doc; }

  bool next() override {
    if (!_front->next()) [[unlikely]] {
      return false;
    }

    return !doc_limits::eof(converge(*_front_doc));
  }

  doc_id_t seek(doc_id_t target) override {
    target = _front->seek(target);
    if (doc_limits::eof(target)) [[unlikely]] {
      return doc_limits::eof();
    }

    return converge(target);
  }

 private:
  // tries to converge front_ and other iterators to the specified target.
  // if it impossible tries to find first convergence place
  doc_id_t converge(doc_id_t target) {
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
  DocIterator* _front;
  const doc_id_t* _front_doc{};
};

template<bool Root, typename DocIteratorImpl, typename Merger>
class BlockConjunction : public ConjunctionBase<DocIteratorImpl, Merger> {
  using Base = ConjunctionBase<DocIteratorImpl, Merger>;
  using Attributes =
    std::tuple<DocAttr, AttributePtr<CostAttr>, irs::ScoreAttr>;

 public:
  explicit BlockConjunction(Merger&& merger,
                            std::vector<DocIteratorImpl>&& itrs,
                            SubScores&& scores, bool strict)
    : Base{std::move(merger), std::move(itrs), std::move(scores.scores)},
      _sum_scores{scores.sum_score},
      _score{static_cast<Merger&>(*this).size()} {
    SDB_ASSERT(this->_itrs.size() >= 2);
    SDB_ASSERT(!this->_scores.empty());
    absl::c_sort(this->_scores, [](const auto* lhs, const auto* rhs) {
      return lhs->max.tail > rhs->max.tail;
    });
    std::get<AttributePtr<CostAttr>>(_attrs) =
      irs::GetMutable<CostAttr>(this->_itrs.front().it.get());
    auto& score = std::get<irs::ScoreAttr>(_attrs);
    score.max.leaf = score.max.tail = _sum_scores;
    auto min = strict ? MinStrictN : MinWeakN;
    if constexpr (Root) {
      auto score_root = [](ScoreCtx* ctx, score_t* res) noexcept {
        auto& self = static_cast<BlockConjunction&>(*ctx);
        std::memcpy(res, self._score.temp(),
                    static_cast<Merger&>(self).byte_size());
      };
      this->PrepareScore(score, score_root, score_root, min);
    } else {
      this->PrepareScore(score, Base::Score2, Base::ScoreN, min);
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  IRS_FORCE_INLINE auto& score() {
    return std::get<irs::ScoreAttr>(_attrs).max;
  }

  doc_id_t value() const final { return std::get<DocAttr>(_attrs).value; }

  bool next() final { return !doc_limits::eof(seek(value() + 1)); }

  doc_id_t shallow_seek(doc_id_t target) final {
    target = ShallowSeekImpl(target);
    if (doc_limits::eof(target)) [[unlikely]] {
      return Seal();
    }
    return _leafs_doc;
  }

  doc_id_t seek(doc_id_t target) final {
    auto& doc = std::get<DocAttr>(_attrs).value;
    if (target <= doc) [[unlikely]] {
      if constexpr (Root) {
        if (_threshold < _score.temp()[0]) {
          return doc;
        }
        target = doc + !doc_limits::eof(doc);
      } else {
        return doc;
      }
    }
    auto& merger = static_cast<Merger&>(*this);
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

    if constexpr (Root) {
      auto begin = this->_scores.begin();
      auto end = this->_scores.end();

      (**begin)(_score.temp());
      for (++begin; begin != end; ++begin) {
        (**begin)(merger.temp());
        merger(_score.temp(), merger.temp());
      }
      if (_threshold < _score.temp()[0]) {
        return target;
      }
      ++target;
      if (target > _leafs_doc) {
        goto align_leafs;
      }
      goto align_docs;
    } else {
      return target;
    }
  }

 private:
  // TODO(mbkkt) Maybe optimize for 2?
  static void MinN(BlockConjunction& self, score_t arg) noexcept {
    for (auto* score : self._scores) {
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
    auto& merger = static_cast<Merger&>(*this);
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
      merger.Merge(sum_leafs_score, it.score->max.leaf);
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
  typename Merger::Buffer _score;
};

// Returns conjunction iterator created from the specified sub iterators
template<template<typename> typename Wrapper = EmptyWrapper, typename Merger,
         typename DocIteratorImpl, typename... Args>
DocIterator::ptr MakeConjunction(WandContext ctx, Merger&& merger,
                                 std::vector<DocIteratorImpl>&& itrs,
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
  using ConjunctionImpl = Conjunction<DocIteratorImpl, Merger>;
  using WrappedConjunction = Wrapper<ConjunctionImpl>;
  if constexpr (kHasScore<Merger> &&
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
          Wrapper<BlockConjunction<Root, DocIteratorImpl, Merger>>>(
          std::forward<Args>(args)..., std::forward<Merger>(merger),
          std::move(itrs), std::move(scores), ctx.strict);
      });
    }
    // TODO(mbkkt) We still could set min producer and root scoring
  }

  return memory::make_managed<Wrapper<Conjunction<DocIteratorImpl, Merger>>>(
    std::forward<Args>(args)..., std::forward<Merger>(merger), std::move(itrs),
    std::move(scores.scores));
}

}  // namespace irs
