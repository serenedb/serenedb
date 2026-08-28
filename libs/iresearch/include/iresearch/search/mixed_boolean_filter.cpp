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

#include "mixed_boolean_filter.hpp"

#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/term_filter.hpp"

namespace irs {
namespace {

bool SideEmpty(const Filter& side) noexcept {
  const auto tid = side.type();
  if (tid == irs::Type<Empty>::id()) {
    return true;
  }
  if (tid == irs::Type<And>::id() || tid == irs::Type<Or>::id()) {
    return sdb::basics::downCast<BooleanFilter>(side).empty();
  }
  return false;
}

const Or* AsOr(const Filter& side) noexcept {
  return side.type() == irs::Type<Or>::id() ? &sdb::basics::downCast<Or>(side)
                                            : nullptr;
}

const ByTerms* AsSplittableByTerms(const Filter& side) noexcept {
  if (side.type() != irs::Type<ByTerms>::id()) {
    return nullptr;
  }
  const auto& terms = sdb::basics::downCast<ByTerms>(side);
  const auto& options = terms.options();
  if (options.min_match > 1 || options.merge_type != ScoreMergeType::Sum) {
    return nullptr;
  }
  return &terms;
}

}  // namespace

PrepareCollector::ptr MixedBooleanFilter::MakeCollectorImpl(
  const Scorer* scorer) const {
  const auto& req = RequiredSlot();
  const auto& opt = OptionalSlot();
  if (SideEmpty(*req)) {
    return opt->MakeCollector(scorer);
  }
  if (SideEmpty(*opt)) {
    return req->MakeCollector(scorer);
  }
  auto compound = std::make_unique<CompoundCollector>(scorer);
  compound->Add(req->MakeCollector(scorer));
  if (const auto* opt_or = AsOr(*opt)) {
    for (const auto& clause : *opt_or) {
      compound->Add(clause->MakeCollector(scorer));
    }
  } else if (const auto* opt_terms = AsSplittableByTerms(*opt)) {
    for (size_t i = 0, count = opt_terms->options().terms.size(); i != count;
         ++i) {
      compound->Add(std::make_unique<ByTermsCollector>(scorer, 1));
    }
  } else {
    compound->Add(opt->MakeCollector(scorer));
  }
  return compound;
}

QueryBuilder::ptr MixedBooleanFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& req = RequiredSlot();
  const auto& opt = OptionalSlot();
  if (SideEmpty(*req)) {
    return opt->PrepareSegment(segment, ctx);
  }
  if (SideEmpty(*opt)) {
    return req->PrepareSegment(segment, ctx);
  }

  auto* compound = dynamic_cast<CompoundCollector*>(ctx.collector);
  SDB_ASSERT(ctx.collector == nullptr || compound != nullptr);

  size_t idx = 0;
  PrepareContext req_ctx = ctx;
  req_ctx.collector = compound ? &compound->Child(idx++) : nullptr;
  auto req_query = req->PrepareSegment(segment, req_ctx);

  std::vector<QueryBuilder::ptr> opt_queries;
  const auto add_clause = [&](const Filter& clause, score_t boost) {
    PrepareContext child = ctx;
    child.boost = ctx.boost * boost;
    child.collector = compound ? &compound->Child(idx++) : nullptr;
    opt_queries.emplace_back(clause.PrepareSegment(segment, child));
  };
  if (const auto* opt_or = AsOr(*opt)) {
    opt_queries.reserve(opt_or->size());
    for (const auto& clause : *opt_or) {
      add_clause(*clause, opt_or->GetBoost());
    }
  } else if (const auto* opt_terms = AsSplittableByTerms(*opt)) {
    const auto& terms = opt_terms->options().terms;
    const auto field = opt_terms->field_id();
    const auto boost = ctx.boost * opt_terms->GetBoost();
    opt_queries.reserve(terms.size());
    for (const auto& term : terms) {
      PrepareContext child = ctx;
      child.boost = boost * term.boost;
      child.collector = compound ? &compound->Child(idx++) : nullptr;
      opt_queries.emplace_back(
        ByTerm::PrepareSegment(segment, child, field, term.term));
    }
  } else {
    add_clause(*opt, kNoBoost);
  }

  return memory::make_tracked<BoostQuery>(
    ctx.memory, segment, std::move(req_query), std::move(opt_queries));
}

bool MixedBooleanFilter::equals(const Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<MixedBooleanFilter>(rhs);
  return *RequiredSlot() == *typed_rhs.RequiredSlot() &&
         *OptionalSlot() == *typed_rhs.OptionalSlot();
}

}  // namespace irs
