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
#include <ranges>

#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/iterators.hpp"
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

  ScoreAdapter(DocIterator::ptr it) noexcept : _it{std::move(it)} {
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

  IRS_FORCE_INLINE uint32_t count() { return _it->count(); }

  IRS_FORCE_INLINE void FetchScoreArgs(uint16_t index) {
    return _it->FetchScoreArgs(index);
  }

  IRS_FORCE_INLINE ScoreFunction
  PrepareScore(const PrepareScoreContext& ctx) const noexcept {
    return _it->PrepareScore(ctx);
  }

  IRS_FORCE_INLINE std::pair<doc_id_t, bool> FillBlock(
    doc_id_t min, doc_id_t max, uint64_t* mask, CollectScoreContext score,
    CollectMatchContext match) {
    return _it->FillBlock(min, max, mask, score, match);
  }

 private:
  DocIterator::ptr _it;
  const doc_id_t* _doc{};
};

using ScoreAdapters = std::vector<ScoreAdapter>;

template<typename T>
using EmptyWrapper = T;

class ConjunctionScorer : public ScoreCtx {
 public:
  static ScoreFunction Make(ScoreMergeType merge_type,
                            const PrepareScoreContext& ctx, auto& itrs,
                            auto min) {
    if (merge_type == ScoreMergeType::Noop) {
      return ScoreFunction::Default();
    }

    std::vector<ScoreFunction> sources;
    sources.reserve(itrs.size());
    for (auto& it : itrs) {
      auto score = it.PrepareScore(ctx);
      if (score.IsDefault()) {
        continue;
      }
      sources.emplace_back(std::move(score));
    }

    if (sources.empty()) {
      return ScoreFunction::Default();
    }

    return ResolveMergeType(merge_type, [&]<ScoreMergeType MergeType> {
      return ScoreFunction::Make<ConjunctionScorer>(
        [](ScoreCtx* ctx, score_t* res, size_t n) noexcept {
          auto& self = *static_cast<ConjunctionScorer*>(ctx);
          auto source = self._sources.begin();
          auto end = self._sources.end();

          source->Score(res, n);
          for (++source; source != end; ++source) {
            source->Score(self._scores.data(), n);
            Merge<MergeType>(res, self._scores.data(), n);
          }
        },
        min, std::move(sources));
    });
  }

  explicit ConjunctionScorer(std::vector<ScoreFunction> sources)
    : _sources{std::move(sources)} {}

 private:
  std::vector<ScoreFunction> _sources;
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
  void FetchScoreArgs(uint16_t index) final {
    for (auto& it : _itrs) {
      it.FetchScoreArgs(index);
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
  using Attributes = std::tuple<AttributePtr<DocAttr>, AttributePtr<CostAttr>>;

 public:
  explicit Conjunction(ScoreMergeType merge_type, std::vector<Adapter>&& itrs)
    : Base{merge_type, std::move(itrs)} {
    SDB_ASSERT(!this->_itrs.empty());

    std::get<AttributePtr<DocAttr>>(_attrs) =
      irs::GetMutable<DocAttr>(&this->_itrs[0]);
    std::get<AttributePtr<CostAttr>>(_attrs) =
      irs::GetMutable<CostAttr>(&this->_itrs[0]);
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    return ConjunctionScorer::Make(this->_merge_type, ctx, this->_itrs,
                                   ScoreFunction::NoopMin);
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

  uint32_t Collect(const ScoreFunction& scorer, ColumnCollector& columns,
                   std::span<doc_id_t, kScoreBlock> docs,
                   std::span<score_t, kScoreBlock> scores) final {
    return DocIterator::Collect(*this, scorer, columns, docs, scores);
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

  absl::c_sort(itrs, [](const auto& lhs, const auto& rhs) noexcept {
    return CostAttr::extract(lhs, CostAttr::kMax) <
           CostAttr::extract(rhs, CostAttr::kMax);
  });

  return memory::make_managed<Wrapper<Conjunction<Adapter>>>(
    merge_type, std::forward<Args>(args)..., std::move(itrs));
}

}  // namespace irs
