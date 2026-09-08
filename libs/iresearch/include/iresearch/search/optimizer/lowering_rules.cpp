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

#include "iresearch/search/optimizer/lowering_rules.hpp"

#include <algorithm>
#include <cmath>
#include <memory>
#include <type_traits>
#include <variant>

#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/ngram_similarity_filter.hpp"
#include "iresearch/search/optimizer/boolean_rules.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_set.hpp"
#include "iresearch/search/wildcard_filter.hpp"

namespace irs::optimizer {
namespace {

struct WildcardSimplifyRule {
  static constexpr std::string_view kName = "wildcard_simplify";
  static constexpr std::array kTargets{Type<ByWildcard>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct RegexpSimplifyRule {
  static constexpr std::string_view kName = "regexp_simplify";
  static constexpr std::array kTargets{Type<ByRegexp>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct EditDistanceSimplifyRule {
  static constexpr std::string_view kName = "edit_distance_simplify";
  static constexpr std::array kTargets{Type<ByEditDistance>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct PhraseLowerRule {
  static constexpr std::string_view kName = "phrase_lower";
  static constexpr std::array kTargets{Type<ByPhrase>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct PhraseSimplifyRule {
  static constexpr std::string_view kName = "phrase_simplify";
  static constexpr std::array kTargets{Type<ByPhrase>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct NGramSimilarityLowerRule {
  static constexpr std::string_view kName = "ngram_similarity_lower";
  static constexpr std::array kTargets{Type<ByNGramSimilarity>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

}  // namespace

bool WildcardSimplifyRule::Apply(Filter::ptr& slot, const OptimizeContext&) {
  auto& node = sdb::basics::downCast<ByWildcard>(*slot);
  bstring buf;
  auto lowered = ExecuteWildcard(
    buf, node.options().term,
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByTerm>();
      *filter->mutable_field_id() = node.field_id();
      filter->mutable_options()->term = term;
      filter->SetBoost(node.GetBoost());
      filter->SetScorer(node.GetScorer());
      return filter;
    },
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByPrefix>();
      *filter->mutable_field_id() = node.field_id();
      filter->mutable_options()->term = term;
      filter->SetBoost(node.GetBoost());
      filter->SetScorer(node.GetScorer());
      return filter;
    },
    [](bytes_view) -> Filter::ptr { return nullptr; });
  if (lowered == nullptr) {
    return false;
  }
  slot = std::move(lowered);
  return true;
}

bool RegexpSimplifyRule::Apply(Filter::ptr& slot, const OptimizeContext&) {
  auto& node = sdb::basics::downCast<ByRegexp>(*slot);
  bstring buf;
  auto lowered = ExecuteRegexp(
    buf, node.options().pattern,
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByTerm>();
      *filter->mutable_field_id() = node.field_id();
      filter->mutable_options()->term = term;
      filter->SetBoost(node.GetBoost());
      filter->SetScorer(node.GetScorer());
      return filter;
    },
    [&](bytes_view prefix) -> Filter::ptr {
      auto filter = std::make_unique<ByPrefix>();
      *filter->mutable_field_id() = node.field_id();
      filter->mutable_options()->term = prefix;
      filter->SetBoost(node.GetBoost());
      filter->SetScorer(node.GetScorer());
      return filter;
    },
    [](bytes_view) -> Filter::ptr { return nullptr; });
  if (lowered == nullptr) {
    return false;
  }
  slot = std::move(lowered);
  return true;
}

bool EditDistanceSimplifyRule::Apply(Filter::ptr& slot,
                                     const OptimizeContext&) {
  auto& node = sdb::basics::downCast<ByEditDistance>(*slot);
  const auto& opts = node.options();
  if (opts.max_distance != 0) {
    return false;
  }
  auto filter = std::make_unique<ByTerm>();
  *filter->mutable_field_id() = node.field_id();
  auto& target = filter->mutable_options()->term;
  target.reserve(opts.prefix.size() + opts.term.size());
  target += opts.prefix;
  target += opts.term;
  filter->SetBoost(node.GetBoost());
  filter->SetScorer(node.GetScorer());
  slot = std::move(filter);
  return true;
}

bool PhraseLowerRule::Apply(Filter::ptr& slot, const OptimizeContext&) {
  return sdb::basics::downCast<ByPhrase>(*slot).mutable_options()->LowerParts();
}

bool PhraseSimplifyRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& phrase = sdb::basics::downCast<ByPhrase>(*slot);
  if (phrase.options().size() != 1) {
    return false;
  }
  const auto field = phrase.field_id();
  const auto boost = phrase.GetBoost();
  const auto* scorer = phrase.GetScorer();
  auto lowered = std::visit(
    [&]<typename Options>(Options& options) -> Filter::ptr {
      using Opts = std::remove_cvref_t<Options>;
      if constexpr (std::is_same_v<Opts, TermSetOptions>) {
        if (options.terms.empty()) {
          return std::make_unique<Empty>();
        }
        SDB_ASSERT(options.min_match == 1);
        if (ctx.scored) {
          return nullptr;
        }
        auto node = std::make_unique<BooleanFilter>();
        node->SetMergeType(options.merge_type);
        for (auto& term : options.terms) {
          node->Add(
            TermClause{.field = field, .term = term.term, .boost = term.boost},
            Occur::Should);
        }
        node->SetMinShouldMatch(1);
        node->SetBoost(boost);
        node->SetScorer(scorer);
        return node;
      } else {
        auto node = std::make_unique<typename Opts::FilterType>();
        *node->mutable_field_id() = field;
        *node->mutable_options() = std::move(options);
        node->SetBoost(boost);
        node->SetScorer(scorer);
        return node;
      }
    },
    phrase.mutable_options()->begin()->part);
  if (!lowered) {
    return false;
  }
  slot = std::move(lowered);
  return true;
}

bool NGramSimilarityLowerRule::Apply(Filter::ptr& slot,
                                     const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<ByNGramSimilarity>(*slot);
  const auto& ngrams = node.options().ngrams;
  if (ngrams.empty()) {
    slot = std::make_unique<Empty>();
    return true;
  }
  const auto terms_count = ngrams.size();
  const auto threshold = std::clamp(node.options().threshold, 0.F, 1.F);
  const auto min_match =
    std::clamp(static_cast<size_t>(
                 std::ceil(static_cast<float_t>(terms_count) * threshold)),
               size_t{1}, terms_count);
  if (terms_count == 1) {
    auto by_term = std::make_unique<ByTerm>();
    *by_term->mutable_field_id() = node.field_id();
    by_term->mutable_options()->term = ngrams.front();
    by_term->SetBoost(node.GetBoost());
    by_term->SetScorer(node.GetScorer());
    slot = std::move(by_term);
    return true;
  }
  if (!ctx.scored && min_match == 1) {
    auto disjunction_node = std::make_unique<BooleanFilter>();
    for (const auto& ngram : ngrams) {
      disjunction_node->Add(TermClause{.field = node.field_id(), .term = ngram},
                            Occur::Should);
    }
    disjunction_node->SetMinShouldMatch(1);
    disjunction_node->SetBoost(node.GetBoost());
    disjunction_node->SetScorer(node.GetScorer());
    slot = std::move(disjunction_node);
    return true;
  }
  if (node.options().allow_phrase && min_match == terms_count &&
      terms_count >= 2) {
    auto by_phrase = std::make_unique<ByPhrase>();
    *by_phrase->mutable_field_id() = node.field_id();
    auto* options = by_phrase->mutable_options();
    for (const auto& ngram : ngrams) {
      options->push_back(ByTermOptions{ngram});
    }
    by_phrase->SetBoost(node.GetBoost());
    by_phrase->SetScorer(node.GetScorer());
    slot = std::move(by_phrase);
    return true;
  }
  return false;
}

void InitWildcardSimplify() { RegisterRule<WildcardSimplifyRule>(); }

void InitRegexpSimplify() { RegisterRule<RegexpSimplifyRule>(); }

void InitEditDistanceSimplify() { RegisterRule<EditDistanceSimplifyRule>(); }

void InitPhraseSimplify() { RegisterRule<PhraseSimplifyRule>(); }

void InitPhraseLower() { RegisterRule<PhraseLowerRule>(); }

void InitNGramSimilarityLower() { RegisterRule<NGramSimilarityLowerRule>(); }

namespace {

void LowerNode(Filter::ptr& slot) {
  const auto type = slot->type();
  if (type == Type<ByWildcard>::id()) {
    auto& node = sdb::basics::downCast<ByWildcard>(*slot);
    slot = LowerWildcard(node.field_id(), node.options().term, node.GetBoost());
  } else if (type == Type<ByRegexp>::id()) {
    auto& node = sdb::basics::downCast<ByRegexp>(*slot);
    slot = LowerRegexp(node.field_id(), node.options().pattern,
                       node.options().syntax, node.GetBoost());
  } else if (type == Type<ByEditDistance>::id()) {
    auto& node = sdb::basics::downCast<ByEditDistance>(*slot);
    slot = LowerLevenshtein(node.field_id(), node.options(), node.GetBoost());
  }
}

}  // namespace

void LowerAutomatons(Filter::ptr& root, const OptimizeContext& ctx) {
  TraverseFilter(root, [&](Filter::ptr& slot) {
    LowerNode(slot);
    if (slot->type() == Type<BooleanFilter>::id()) {
      NormalizeTerms(sdb::basics::downCast<BooleanFilter>(*slot), ctx.scored);
    }
  });
}

}  // namespace irs::optimizer
