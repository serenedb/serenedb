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

  IRS_FORCE_INLINE auto seek_to_leaf(doc_id_t target) {
    return _it->seek_to_leaf(target);
  }

  IRS_FORCE_INLINE doc_id_t seek_in_leaf(doc_id_t target) {
    return _it->seek_in_leaf(target);
  }

  IRS_FORCE_INLINE uint32_t count() { return _it->count(); }

  IRS_FORCE_INLINE const ScoreAttr& score() const noexcept { return *_score; }

 private:
  DocIterator::ptr _it;
  const doc_id_t* _doc{};
  const ScoreAttr* _score{};
};

using ScoreAdapters = std::vector<ScoreAdapter>;

// Helpers
template<typename T>
using EmptyWrapper = T;

struct SubScores {
  std::vector<const ScoreAttr*> scores;
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
template<typename Adapter, typename Merger>
struct ConjunctionBase : public DocIterator,
                         protected Merger,
                         protected ScoreCtx {
 protected:
  explicit ConjunctionBase(Merger&& merger, std::vector<Adapter>&& itrs,
                           std::vector<const ScoreAttr*>&& scorers)
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

  void PrepareScore(ScoreAttr& score, auto score_2, auto score_n, auto min) {
    SDB_ASSERT(Merger::size());
    switch (_scores.size()) {
      case 0:
        score = ScoreFunction::Default(Merger::size());
        break;
      case 1:
        score = std::move(*const_cast<ScoreAttr*>(_scores.front()));
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

  std::vector<Adapter> _itrs;
  std::vector<const ScoreAttr*> _scores;
};

template<typename Adapter, typename Merger>
class Conjunction : public ConjunctionBase<Adapter, Merger> {
  using Base = ConjunctionBase<Adapter, Merger>;
  using Attributes =
    std::tuple<AttributePtr<DocAttr>, AttributePtr<CostAttr>, ScoreAttr>;

 public:
  explicit Conjunction(Merger&& merger, std::vector<Adapter>&& itrs,
                       std::vector<const ScoreAttr*>&& scores = {})
    : Base{std::move(merger), std::move(itrs), std::move(scores)},
      _front{this->_itrs.front()} {
    SDB_ASSERT(!this->_itrs.empty());
    SDB_ASSERT(_front);

    std::get<AttributePtr<DocAttr>>(_attrs) = irs::GetMutable<DocAttr>(&_front);
    std::get<AttributePtr<CostAttr>>(_attrs) =
      irs::GetMutable<CostAttr>(&_front);

    if constexpr (kHasScore<Merger>) {
      auto& score = std::get<ScoreAttr>(_attrs);
      this->PrepareScore(score, Base::Score2, Base::ScoreN,
                         ScoreFunction::DefaultMin);
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const final {
    return std::get<AttributePtr<DocAttr>>(_attrs).ptr->value;
  }

  doc_id_t advance() override { return converge(_front.advance()); }

  doc_id_t seek(doc_id_t target) override {
    return converge(_front.seek(target));
  }

 private:
  // tries to converge front_ and other iterators to the specified target.
  // if it impossible tries to find first convergence place
  doc_id_t converge(doc_id_t target) {
    if (doc_limits::eof(target)) [[unlikely]] {
      return doc_limits::kEOF;
    }
    const auto begin = this->_itrs.begin() + 1;
    const auto end = this->_itrs.end();
  restart:
    SDB_ASSERT(!doc_limits::eof(target));
    for (auto it = begin; it != end; ++it) {
      const auto doc = it->seek(target);
      if (target < doc) {
        target = _front.seek(doc);
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

template<typename Adapter>
class BlockConjunctionNoScore
  : public ConjunctionBase<Adapter, NoopAggregator> {
  using Base = ConjunctionBase<Adapter, NoopAggregator>;
  using Attributes = std::tuple<DocAttr, AttributePtr<CostAttr>>;

 public:
  explicit BlockConjunctionNoScore(std::vector<Adapter>&& itrs)
    : Base{NoopAggregator{}, std::move(itrs), {}} {
    SDB_ASSERT(this->_itrs.size() >= 2);
    std::get<AttributePtr<CostAttr>>(_attrs) =
      irs::GetMutable<CostAttr>(&this->_itrs.front());
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const final { return std::get<DocAttr>(_attrs).value; }

  doc_id_t advance() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;
    auto target = doc_value + 1;
  again:
    if (target <= _leafs_max) {
    align_in_leaf:
      for (auto& it : this->_itrs) {
        const auto doc = it.seek_in_leaf(target);
        if (doc != target) {
          target = doc;
          goto again;
        }
      }
      return doc_value = target;
    }
  align_to_leaf:
    if (doc_limits::eof(target)) {
      return doc_value = doc_limits::kEOF;
    }
    auto leafs_min = doc_limits::min();     // included
    auto leafs_max = doc_limits::kEOF - 1;  // included
    for (auto& it : this->_itrs) {
      const auto [leaf_min, leaf_max] = it.seek_to_leaf(target);
      SDB_ASSERT(leaf_min <= leaf_max);
      SDB_ASSERT(target <= leaf_max);
      if (target < leaf_min) {
        target = leaf_min;
      }
      if (leaf_min > leafs_max) {
        goto align_to_leaf;
      }
      if (leafs_min < leaf_min) {
        leafs_min = leaf_min;
      }
      if (leafs_max > leaf_max) {
        leafs_max = leaf_max;
      }
      SDB_ASSERT(leafs_min <= target);
      SDB_ASSERT(target <= leafs_max);
    }
    _leafs_max = leafs_max;
    goto align_in_leaf;
  }

  doc_id_t seek(doc_id_t target) final { return doc_limits::kEOF; }

  uint32_t count() final { return DocIterator::Count(*this); }

 private:
  Attributes _attrs;
  doc_id_t _leafs_max = 0;
};

// Returns conjunction iterator created from the specified sub iterators
template<template<typename> typename Wrapper = EmptyWrapper, typename Merger,
         typename Adapter, typename... Args>
DocIterator::ptr MakeConjunction(WandContext ctx, Merger&& merger,
                                 std::vector<Adapter>&& itrs, Args&&... args) {
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
  using ConjunctionImpl = Conjunction<Adapter, Merger>;
  using WrappedConjunction = Wrapper<ConjunctionImpl>;

  if constexpr (!kHasScore<Merger> &&
                std::is_same_v<ConjunctionImpl, WrappedConjunction>) {
    return memory::make_managed<Wrapper<BlockConjunctionNoScore<Adapter>>>(
      std::move(itrs));
  }

  return memory::make_managed<Wrapper<Conjunction<Adapter, Merger>>>(
    std::forward<Args>(args)..., std::forward<Merger>(merger), std::move(itrs),
    std::move(scores.scores));
}

}  // namespace irs
