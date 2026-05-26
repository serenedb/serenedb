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

#include "boolean_filter.hpp"

#include <absl/container/inlined_vector.h>

#include "basics/down_cast.h"
#include "conjunction.hpp"
#include "disjunction.hpp"
#include "exclusion.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "prepared_state_visitor.hpp"

namespace irs {
namespace {

class CompoundBuffer final : public Filter::ScoredBuffer {
 public:
  enum class Shape : uint8_t { And, Or, MinMatch };

  struct ChildEntry {
    std::unique_ptr<Filter::PrepareBuffer> buffer;
    const Scorer* scorer;
    score_t boost;
  };

  CompoundBuffer(Shape shape, ScoreMergeType merge_type,
                 const PrepareContext& boolean_ctx, size_t min_match = 0)
    : ScoredBuffer{boolean_ctx},
      _shape{shape},
      _merge_type{merge_type},
      _min_match{min_match} {}

  void AddIncl(ChildEntry&& e) {
    SDB_ASSERT(_excl_begin == _children.size());
    _children.push_back(std::move(e));
    ++_excl_begin;
  }

  void AddExcl(ChildEntry&& e) {
    SDB_ASSERT(_excl_begin <= _children.size());
    _children.push_back(std::move(e));
  }

  void AddOwned(AllDocsProvider::Ptr&& f) { _owned.push_back(std::move(f)); }

  void PrepareSegment(const SubReader& segment) final {
    for (auto& c : _children) {
      c.buffer->PrepareSegment(segment);
    }
  }

  void Merge(PrepareBuffer&& other) final {
    auto& rhs = sdb::basics::downCast<CompoundBuffer>(other);
    SDB_ASSERT(_shape == rhs._shape);
    SDB_ASSERT(_merge_type == rhs._merge_type);
    SDB_ASSERT(_min_match == rhs._min_match);
    SDB_ASSERT(_excl_begin == rhs._excl_begin);
    SDB_ASSERT(_children.size() == rhs._children.size());
    for (size_t i = 0; i < _children.size(); ++i) {
      _children[i].buffer->Merge(std::move(*rhs._children[i].buffer));
    }
  }

  bool Empty() const noexcept final { return false; }

  Filter::Query::ptr Compile(const PrepareContext& ctx) && final {
    BooleanQuery::queries_t queries{{ctx.memory}};
    queries.reserve(_children.size());
    for (auto& c : _children) {
      auto buf = std::move(c.buffer);
      if (buf->Empty()) {
        queries.emplace_back(Filter::Query::empty());
        continue;
      }
      PrepareContext child_ctx = ctx;
      child_ctx.scorer = c.scorer;
      child_ctx.boost = c.boost;
      queries.emplace_back(std::move(*buf).Compile(child_ctx));
    }

    memory::managed_ptr<BooleanQuery> q;
    switch (_shape) {
      case Shape::And:
        q = memory::make_tracked<AndQuery>(ctx.memory);
        break;
      case Shape::Or:
        q = memory::make_tracked<OrQuery>(ctx.memory);
        break;
      case Shape::MinMatch:
        q = memory::make_tracked<MinMatchQuery>(ctx.memory, _min_match);
        break;
    }
    PrepareContext boolean_compile_ctx = ctx;
    boolean_compile_ctx.boost = _boost;
    q->prepare(boolean_compile_ctx, _merge_type, std::move(queries),
               _excl_begin);
    return q;
  }

 private:
  Shape _shape;
  ScoreMergeType _merge_type;
  size_t _min_match;
  size_t _excl_begin = 0;
  absl::InlinedVector<ChildEntry, 4> _children;
  absl::InlinedVector<AllDocsProvider::Ptr, 3> _owned;
};

PrepareContext ExclChildCtx(const PrepareContext& parent_ctx) {
  return PrepareContext{
    .index = parent_ctx.index,
    .memory = parent_ctx.memory,
    .ctx = parent_ctx.ctx,
  };
}

CompoundBuffer::ChildEntry MakeChildEntry(const Filter& filter,
                                          const PrepareContext& parent_ctx) {
  return {filter.CreateBuffer(parent_ctx), parent_ctx.scorer, parent_ctx.boost};
}

CompoundBuffer::ChildEntry MakeExclChildEntry(
  const Filter& filter, const PrepareContext& parent_ctx) {
  auto child_ctx = ExclChildCtx(parent_ctx);
  return {filter.CreateBuffer(child_ctx), child_ctx.scorer, child_ctx.boost};
}

std::pair<const Filter*, bool> OptimizeNot(const Not& node) {
  bool neg = true;
  const auto* inner = node.filter();
  while (inner != nullptr && inner->type() == Type<Not>::id()) {
    neg = !neg;
    inner = sdb::basics::downCast<Not>(inner)->filter();
  }

  return std::pair{inner, neg};
}

struct BoolBuild {
  enum class Kind {
    Empty,
    Delegate,
    ByTerms,
    Compound,
  };

  Kind kind = Kind::Empty;

  score_t boost = kNoBoost;

  const Filter* delegate_filter = nullptr;

  std::string_view by_terms_field;
  ByTermsOptions by_terms_options;

  CompoundBuffer::Shape shape = CompoundBuffer::Shape::And;
  size_t min_match = 0;
  ScoreMergeType merge_type = ScoreMergeType::Sum;
  absl::InlinedVector<const Filter*, 4> incl;
  absl::InlinedVector<const Filter*, 1> excl;
  absl::InlinedVector<AllDocsProvider::Ptr, 3> owned;
};

PrepareContext WithBoost(const PrepareContext& ctx, score_t boost) noexcept {
  PrepareContext out = ctx;
  out.boost = boost;
  return out;
}

bool IsOwnedPtr(const Filter* f,
                const absl::InlinedVector<AllDocsProvider::Ptr, 3>& owned) {
  for (const auto& o : owned) {
    if (o.get() == f) {
      return true;
    }
  }
  return false;
}

void GroupFiltersInto(const BooleanFilter& filter,
                      absl::InlinedVector<const Filter*, 4>& incl,
                      absl::InlinedVector<const Filter*, 1>& excl,
                      absl::InlinedVector<AllDocsProvider::Ptr, 3>& owned,
                      AllDocsProvider::Ptr& all_docs_zero_boost,
                      bool& incl_cleared_by_not_all) {
  incl_cleared_by_not_all = false;
  const Filter* empty_filter = nullptr;
  const bool is_or = filter.type() == irs::Type<Or>::id();
  for (const auto& f : filter) {
    if (irs::Type<Empty>::id() == f->type()) {
      empty_filter = f.get();
      continue;
    }
    if (irs::Type<Not>::id() == f->type()) {
      const auto res = OptimizeNot(sdb::basics::downCast<Not>(*f));
      if (!res.first) {
        continue;
      }
      if (res.second) {
        if (!all_docs_zero_boost) {
          all_docs_zero_boost = filter.MakeAllDocsFilter(0.F);
        }
        if (*all_docs_zero_boost == *res.first) {
          incl.clear();
          incl_cleared_by_not_all = true;
          return;
        }
        excl.push_back(res.first);
        if (is_or) {
          // FIXME: this should have same boost as Not filter.
          // But for now we do not boost negation.
          incl.push_back(all_docs_zero_boost.get());
        }
      } else {
        incl.push_back(res.first);
      }
    } else {
      incl.push_back(f.get());
    }
  }
  if (empty_filter != nullptr) {
    incl.push_back(empty_filter);
  }
  if (all_docs_zero_boost) {
    owned.push_back(std::move(all_docs_zero_boost));
  }
}

void AnalyzeAnd(const BooleanFilter& filter, const PrepareContext& ctx,
                BoolBuild& b) {
  auto& incl = b.incl;
  auto& excl = b.excl;
  auto& owned = b.owned;
  b.boost = ctx.boost;

  if (incl.empty() || incl.back()->type() == irs::Type<Empty>::id()) {
    b.kind = BoolBuild::Kind::Empty;
    return;
  }

  if (1 == incl.size() && excl.empty() && !IsOwnedPtr(incl.front(), owned)) {
    b.kind = BoolBuild::Kind::Delegate;
    b.delegate_filter = incl.front();
    return;
  }

  auto cumulative_all = filter.MakeAllDocsFilter(kNoBoost);
  score_t all_boost{0};
  size_t all_count{0};
  for (auto* f : incl) {
    if (*f == *cumulative_all) {
      ++all_count;
      all_boost += sdb::basics::downCast<FilterWithBoost>(*f).Boost();
    }
  }
  const score_t and_boost = filter.Boost();
  if (all_count != 0) {
    const auto non_all_count = incl.size() - all_count;
    auto it = std::remove_if(incl.begin(), incl.end(),
                             [&cumulative_all](const irs::Filter* f) {
                               return *cumulative_all == *f;
                             });
    incl.erase(it, incl.end());
    if (1 == non_all_count) {
      auto left_boost = (*incl.begin())->BoostImpl();
      if (and_boost != 0 && left_boost != 0 && ctx.scorer) {
        b.boost = ctx.boost * (all_boost + left_boost) / left_boost;
      } else {
        b.boost = 0;
      }
    } else {
      cumulative_all->boost(all_boost);
      incl.push_back(cumulative_all.get());
      owned.push_back(std::move(cumulative_all));
    }
  }
  if (1 == incl.size() && excl.empty()) {
    b.kind = BoolBuild::Kind::Delegate;
    b.delegate_filter = incl.front();
    return;
  }
  b.kind = BoolBuild::Kind::Compound;
  b.shape = CompoundBuffer::Shape::And;
}

void AnalyzeOr(const BooleanFilter& filter, const PrepareContext& ctx,
               uint32_t min_match, BoolBuild& b) {
  auto& incl = b.incl;
  auto& excl = b.excl;
  auto& owned = b.owned;
  b.boost = ctx.boost;

  SDB_ASSERT(min_match != 0);

  if (!incl.empty() && incl.back()->type() == irs::Type<Empty>::id()) {
    incl.pop_back();
  }
  if (incl.empty()) {
    b.kind = BoolBuild::Kind::Empty;
    return;
  }
  if (1 == incl.size() && excl.empty() && !IsOwnedPtr(incl.front(), owned)) {
    b.kind = BoolBuild::Kind::Delegate;
    b.delegate_filter = incl.front();
    return;
  }

  auto cumulative_all = filter.MakeAllDocsFilter(kNoBoost);
  size_t optimized_match_count = 0;
  score_t all_boost{0};
  size_t all_count{0};
  const irs::Filter* incl_all{nullptr};
  for (auto* f : incl) {
    if (*f == *cumulative_all) {
      ++all_count;
      all_boost += sdb::basics::downCast<FilterWithBoost>(*f).Boost();
      incl_all = f;
    }
  }
  if (all_count != 0) {
    if (!ctx.scorer && incl.size() > 1 && min_match <= all_count) {
      SDB_ASSERT(incl_all != nullptr);
      incl.resize(1);
      incl.front() = incl_all;
      optimized_match_count = all_count - 1;
    } else {
      auto it = std::remove_if(incl.begin(), incl.end(),
                               [&cumulative_all](const irs::Filter* f) {
                                 return *cumulative_all == *f;
                               });
      incl.erase(it, incl.end());
      cumulative_all->boost(all_boost);
      incl.push_back(cumulative_all.get());
      owned.push_back(std::move(cumulative_all));
      optimized_match_count = all_count - 1;
    }
  }
  const auto adjusted_min_match =
    (optimized_match_count < min_match) ? min_match - optimized_match_count : 1;
  if (adjusted_min_match > incl.size()) {
    b.kind = BoolBuild::Kind::Empty;
    return;
  }
  if (1 == incl.size() && excl.empty()) {
    b.kind = BoolBuild::Kind::Delegate;
    b.delegate_filter = incl.front();
    return;
  }
  SDB_ASSERT(adjusted_min_match > 0 && adjusted_min_match <= incl.size());

  b.kind = BoolBuild::Kind::Compound;
  if (adjusted_min_match == incl.size()) {
    b.shape = CompoundBuffer::Shape::And;
  } else if (1 == adjusted_min_match) {
    b.shape = CompoundBuffer::Shape::Or;
  } else {
    b.shape = CompoundBuffer::Shape::MinMatch;
    b.min_match = adjusted_min_match;
  }
}

BoolBuild AnalyzeBoolean(const BooleanFilter& filter, bool is_or,
                         uint32_t min_match, const PrepareContext& ctx) {
  BoolBuild b;
  b.boost = ctx.boost;
  b.merge_type = filter.merge_type();

  const auto size = filter.size();
  if (size == 0) {
    b.kind = BoolBuild::Kind::Empty;
    return b;
  }

  if (size == 1) {
    auto& f0 = filter[0];
    // FIXME(gnusi): let Not handle everything?
    if (f0.type() != irs::Type<Not>::id()) {
      b.kind = BoolBuild::Kind::Delegate;
      b.delegate_filter = &f0;
      return b;
    }
  }

  if (min_match != 0 && absl::c_all_of(filter, [&](const auto& f) {
        if (f->type() != irs::Type<ByTerm>::id()) {
          return false;
        }
        auto& first = sdb::basics::downCast<ByTerm>(filter[0]);
        auto& cur = sdb::basics::downCast<ByTerm>(*f);
        return first.field() == cur.field();
      })) {
    auto& first = sdb::basics::downCast<ByTerm>(filter[0]);
    ByTermsOptions options;
    options.merge_type = filter.merge_type();
    bool has_duplicates = false;
    for (const auto& f : filter) {
      auto& tf = sdb::basics::downCast<ByTerm>(*f);
      auto it = options.terms.emplace(tf.options().term, tf.Boost());
      if (!it.second) {
        const_cast<score_t&>(it.first->boost) *= tf.Boost();
        has_duplicates = true;
      }
    }
    if (!has_duplicates || min_match == 1 ||
        min_match == std::numeric_limits<uint32_t>::max()) {
      options.min_match = min_match == std::numeric_limits<uint32_t>::max()
                            ? options.terms.size()
                            : min_match;
      b.kind = BoolBuild::Kind::ByTerms;
      b.by_terms_field = first.field();
      b.by_terms_options = std::move(options);
      return b;
    }
  }

  AllDocsProvider::Ptr all_docs_zero_boost;
  bool cleared = false;
  GroupFiltersInto(filter, b.incl, b.excl, b.owned, all_docs_zero_boost,
                   cleared);

  if (cleared) {
    b.kind = BoolBuild::Kind::Empty;
    return b;
  }

  if (b.incl.empty() && !b.excl.empty()) {
    auto all = filter.MakeAllDocsFilter(kNoBoost);
    b.incl.push_back(all.get());
    b.owned.push_back(std::move(all));
  }

  if (is_or) {
    AnalyzeOr(filter, ctx, min_match, b);
  } else {
    AnalyzeAnd(filter, ctx, b);
  }
  return b;
}

void PopulateCompound(BoolBuild&& b, const PrepareContext& effective_ctx,
                      CompoundBuffer& buf) {
  for (auto* f : b.incl) {
    buf.AddIncl(MakeChildEntry(*f, effective_ctx));
  }
  for (auto* f : b.excl) {
    buf.AddExcl(MakeExclChildEntry(*f, effective_ctx));
  }
  for (auto& o : b.owned) {
    buf.AddOwned(std::move(o));
  }
}

std::unique_ptr<Filter::PrepareBuffer> BuildCompound(
  BoolBuild&& b, const PrepareContext& caller_ctx) {
  PrepareContext eff = WithBoost(caller_ctx, b.boost);
  auto buf =
    std::make_unique<CompoundBuffer>(b.shape, b.merge_type, eff, b.min_match);
  PopulateCompound(std::move(b), eff, *buf);
  return buf;
}

}  // namespace

bool BooleanFilter::equals(const Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<BooleanFilter>(rhs);
  return absl::c_equal(*this, typed_rhs, [](const auto& lhs, const auto& rhs) {
    return *lhs == *rhs;
  });
}

std::unique_ptr<Filter::PrepareBuffer> BooleanFilter::CreateBuffer(
  const PrepareContext& raw_ctx) const {
  const auto ctx = raw_ctx.Boost(Boost());
  const bool is_or = type() == Type<Or>::id();
  const uint32_t min_match =
    is_or ? sdb::basics::downCast<Or>(*this).min_match_count()
          : std::numeric_limits<uint32_t>::max();

  auto b = AnalyzeBoolean(*this, is_or, min_match, ctx);
  switch (b.kind) {
    case BoolBuild::Kind::Empty:
      return std::make_unique<EmptyBuffer>();
    case BoolBuild::Kind::Delegate:
      return b.delegate_filter->CreateBuffer(WithBoost(ctx, b.boost));
    case BoolBuild::Kind::ByTerms:
      return ByTerms::CreateBuffer(WithBoost(ctx, b.boost), b.by_terms_field,
                                   std::move(b.by_terms_options));
    case BoolBuild::Kind::Compound:
      return BuildCompound(std::move(b), ctx);
  }
  SDB_ASSERT(false);
  return std::make_unique<EmptyBuffer>();
}

Filter::Query::ptr And::prepare(const PrepareContext& ctx) const {
  return PrepareWithBuffer(*CreateBuffer(ctx), ctx);
}

Filter::Query::ptr Or::prepare(const PrepareContext& ctx) const {
  return PrepareWithBuffer(*CreateBuffer(ctx), ctx);
}

std::unique_ptr<Filter::PrepareBuffer> Or::CreateBuffer(
  const PrepareContext& ctx) const {
  if (0 == _min_match_count) {
    return MakeAllDocsFilter(kNoBoost)->CreateBuffer(ctx.Boost(Boost()));
  }
  return BooleanFilter::CreateBuffer(ctx);
}

Filter::Query::ptr Not::prepare(const PrepareContext& ctx) const {
  return PrepareWithBuffer(*CreateBuffer(ctx), ctx);
}

std::unique_ptr<Filter::PrepareBuffer> Not::CreateBuffer(
  const PrepareContext& raw_ctx) const {
  const auto res = OptimizeNot(*this);
  if (!res.first) {
    return std::make_unique<EmptyBuffer>();
  }

  const auto ctx = raw_ctx.Boost(Boost());

  if (!res.second) {
    return res.first->CreateBuffer(ctx);
  }

  auto all_docs = MakeAllDocsFilter(kNoBoost);
  auto compound = std::make_unique<CompoundBuffer>(CompoundBuffer::Shape::And,
                                                   ScoreMergeType::Sum, ctx);
  compound->AddIncl(MakeChildEntry(*all_docs, ctx));
  compound->AddExcl(MakeExclChildEntry(*res.first, ctx));
  compound->AddOwned(std::move(all_docs));
  return compound;
}

bool Not::equals(const irs::Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<Not>(rhs);
  return (!empty() && !typed_rhs.empty() && *_filter == *typed_rhs._filter) ||
         (empty() && typed_rhs.empty());
}

}  // namespace irs
