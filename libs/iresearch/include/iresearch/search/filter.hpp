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

#include <absl/container/node_hash_map.h>
#include <absl/functional/any_invocable.h>
#include <absl/functional/function_ref.h>

#include <functional>
#include <limits>
#include <span>

#include "basics/down_cast.h"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/constant_score.hpp"
#include "iresearch/search/count/root.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/search/fill/node.hpp"
#include "iresearch/search/lead/node.hpp"
#include "iresearch/search/probe/node.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scored/root.hpp"
#include "iresearch/search/term_iterator.hpp"
#include "iresearch/search/term_predicate.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/search/unscored.hpp"
#include "iresearch/utils/hash_utils.hpp"

namespace irs {

struct IndexReader;
struct PreparedStateVisitor;

struct PrepareContext {
  PrepareCollector* collector = nullptr;
  IResourceManager& memory = IResourceManager::gNoop;
  const AttributeProvider* ctx = nullptr;
  const DocumentMask* pending_docs_mask = nullptr;
  score_t boost = kNoBoost;
  uint32_t thread = 0;
  bool needs_terms = false;

  void Boost(score_t b) noexcept { boost *= b; }

  bool KeepsTerms() const noexcept {
    return collector != nullptr || needs_terms;
  }

  search::StatsRecord Record() const noexcept {
    return collector != nullptr ? collector->Record() : search::StatsRecord{};
  }
};

enum class QueryKind : uint32_t {
  Other,
  Empty,
  All,
  Term,
  Terms,
  Boolean,
};

class QueryBuilder : public memory::Managed {
 public:
  using ptr = memory::managed_ptr<const QueryBuilder>;

  QueryBuilder(const SubReader& segment) noexcept
    : _segment{segment},
      _estimate_max{static_cast<uint32_t>(segment.docs_count())} {}

  QueryBuilder(const SubReader& segment, uint32_t estimate,
               QueryKind kind) noexcept
    : _segment{segment}, _estimate_max{estimate}, _kind{kind} {
    SDB_ASSERT(estimate <= segment.docs_count());
  }

  const SubReader& Segment() const noexcept { return _segment; }
  QueryKind Kind() const noexcept { return _kind; }
  uint32_t EstimateMax() const noexcept { return _estimate_max; }

  void SetStats(search::StatsRecord stats) noexcept { _stats = stats; }

  search::StatsRecord Stats() const noexcept { return _stats; }

  search::StatsRecord Stats(const search::ScoredCtx& ctx) const noexcept {
    return {
      _stats.stats,
      _stats.scorer != nullptr ? _stats.scorer : ctx.scorer,
    };
  }

  bool Scores() const noexcept {
    return _stats.scorer != nullptr && !IsUnscored(*_stats.scorer);
  }

  ~QueryBuilder() override = default;

  static QueryBuilder::ptr Empty();

  static bool IsEmpty(const QueryBuilder& query) noexcept;

  virtual void Visit(PreparedStateVisitor&, score_t boost) const = 0;

  virtual score_t Boost() const noexcept = 0;

  virtual void SetBoost(score_t boost) noexcept {
    SDB_ASSERT(boost == kNoBoost);
  }

  virtual count::Root::ptr PlanCount(const count::Context& ctx) const = 0;

  virtual docs::Root::ptr PlanDocs(const docs::Context& ctx) const = 0;

  virtual scored::Root::ptr PlanScored(const scored::Context& ctx) const = 0;

  virtual top::Root::ptr PlanTop(const top::Context& ctx) const = 0;

  virtual lead::Node::ptr PlanLead(const search::ScoredCtx& ctx) const = 0;

  virtual probe::Node::ptr PlanProbe(const search::ScoredCtx& ctx,
                                     uint64_t interrogations) const = 0;

  virtual fill::Node::ptr PlanFill(const search::ScoredCtx& ctx,
                                   ScoreMergeType merge) const = 0;

 protected:
  const SubReader& _segment;
  uint32_t _estimate_max = 0;
  QueryKind _kind = QueryKind::Other;

 private:
  search::StatsRecord _stats;
};

class Filter {
 public:
  using ptr = std::unique_ptr<Filter>;

  virtual ~Filter() = default;

  IRS_FORCE_INLINE bool operator==(const Filter& rhs) const noexcept {
    return equals(rhs);
  }

  virtual QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                           const PrepareContext& ctx) const = 0;

  const Scorer* GetScorer() const noexcept { return _scorer; }

  void SetScorer(const Scorer* scorer) noexcept { _scorer = scorer; }

  score_t GetBoost() const noexcept { return _boost; }

  void SetBoost(score_t boost) noexcept { _boost = boost; }

  PrepareCollector::ptr MakeCollector(const Scorer& scorer, StatsArena& stats,
                                      uint32_t threads) const {
    SDB_ASSERT(!IsUnscored(scorer));
    const auto* const own = ResolveScorer(_scorer, &scorer);
    if (IsUnscored(*own)) {
      return nullptr;
    }
    return MakeCollectorImpl(own, stats, threads);
  }

  virtual TypeInfo::type_id type() const noexcept = 0;

  using ChildVisitor = absl::FunctionRef<void(Filter::ptr&)>;

  virtual void VisitChildren(ChildVisitor) {}

  virtual TermPredicate::ptr CompileTermPredicate() const { return nullptr; }

  virtual TermIterator::ptr CompileTermIterator(const TermReader& reader) const;

  static Filter::ptr empty();

 protected:
  virtual PrepareCollector::ptr MakeCollectorImpl(const Scorer* scorer,
                                                  StatsArena& stats,
                                                  uint32_t threads) const;

  virtual bool equals(const Filter& rhs) const noexcept {
    return type() == rhs.type();
  }

 private:
  const Scorer* _scorer = nullptr;
  score_t _boost = kNoBoost;
};

class PreparedCollector {
 public:
  PreparedCollector(const Filter& filter, const Scorer& scorer,
                    StatsArena& stats, uint32_t threads)
    : _stats{stats}, _root{filter.MakeCollector(scorer, stats, threads)} {}

  PrepareCollector* Get() const noexcept { return _root.get(); }

  const Scorer* GetScorer() const noexcept {
    return _root != nullptr ? _root->GetScorer() : nullptr;
  }

  void Finish() {
    if (_root != nullptr) {
      _root->Finish(_stats);
    }
  }

 private:
  StatsArena& _stats;
  PrepareCollector::ptr _root;
};

template<typename Type>
class FilterWithType : public Filter {
 public:
  using FilterType = Type;

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<Type>::id();
  }
};

template<typename Options>
class FilterWithOptions : public FilterWithType<typename Options::FilterType> {
 public:
  using options_type = Options;
  using FilterType = typename options_type::FilterType;

  const options_type& options() const noexcept { return _options; }
  options_type* mutable_options() noexcept { return &_options; }

 protected:
  bool equals(const Filter& rhs) const noexcept override {
    return Filter::equals(rhs) &&
           _options == sdb::basics::downCast<FilterType>(rhs)._options;
  }

 private:
  [[no_unique_address]] options_type _options;
};

template<typename Options>
class FilterWithField : public FilterWithOptions<Options> {
 public:
  using options_type = typename FilterWithOptions<Options>::options_type;
  using FilterType = typename options_type::FilterType;

  irs::field_id field_id() const noexcept { return _field_id; }
  irs::field_id* mutable_field_id() noexcept { return &_field_id; }

 protected:
  bool equals(const Filter& rhs) const noexcept final {
    if (!FilterWithOptions<options_type>::equals(rhs)) {
      return false;
    }
    const auto& r = sdb::basics::downCast<FilterType>(rhs);
    return _field_id == r._field_id;
  }

 private:
  irs::field_id _field_id{irs::field_limits::invalid()};
};

class Empty final : public FilterWithType<Empty> {
 public:
  TermPredicate::ptr CompileTermPredicate() const final {
    return MakeTermPredicate(AcceptNoTerms{});
  }

 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;
};

struct FilterVisitor;
using field_visitor =
  absl::AnyInvocable<void(const SubReader&, const TermReader&, FilterVisitor&)>;

}  // namespace irs
