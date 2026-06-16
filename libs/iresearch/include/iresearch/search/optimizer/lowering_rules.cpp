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

#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/ngram_similarity_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
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

bool WildcardSimplifyRule::Apply(Filter::ptr& slot,
                                 const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<ByWildcard>(*slot);
  bstring buf;
  auto lowered = ExecuteWildcard(
    buf, node.options().term,
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByTerm>();
      *filter->mutable_field_id() = node.field_id();
      filter->mutable_options()->term = term;
      filter->boost(node.Boost());
      return filter;
    },
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByPrefix>();
      *filter->mutable_field_id() = node.field_id();
      filter->mutable_options()->term = term;
      filter->mutable_options()->scored_terms_limit =
        node.options().scored_terms_limit;
      filter->boost(node.Boost());
      return filter;
    },
    [](bytes_view) -> Filter::ptr { return nullptr; });
  if (lowered == nullptr) {
    return false;
  }
  slot = std::move(lowered);
  return true;
}

bool RegexpSimplifyRule::Apply(Filter::ptr& slot,
                               const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<ByRegexp>(*slot);
  bstring buf;
  auto lowered = ExecuteRegexp(
    buf, node.options().pattern,
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByTerm>();
      *filter->mutable_field_id() = node.field_id();
      filter->mutable_options()->term = term;
      filter->boost(node.Boost());
      return filter;
    },
    [&](bytes_view prefix) -> Filter::ptr {
      auto filter = std::make_unique<ByPrefix>();
      *filter->mutable_field_id() = node.field_id();
      filter->mutable_options()->term = prefix;
      filter->mutable_options()->scored_terms_limit =
        node.options().scored_terms_limit;
      filter->boost(node.Boost());
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
                                     const OptimizeContext& /*ctx*/) {
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
  filter->boost(node.Boost());
  slot = std::move(filter);
  return true;
}

bool PhraseLowerRule::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
  return sdb::basics::downCast<ByPhrase>(*slot).mutable_options()->LowerParts();
}

bool PhraseSimplifyRule::Apply(Filter::ptr& slot, const OptimizeContext&) {
  auto& phrase = sdb::basics::downCast<ByPhrase>(*slot);
  if (!phrase.options().simple() || phrase.options().size() != 1) {
    return false;
  }
  auto term = std::make_unique<ByTerm>();
  *term->mutable_field_id() = phrase.field_id();
  *term->mutable_options() =
    std::move(std::get<ByTermOptions>(phrase.mutable_options()->begin()->part));
  term->boost(phrase.Boost());
  slot = std::move(term);
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
  if (ctx.scorer == nullptr && min_match == 1) {
    auto by_terms = std::make_unique<ByTerms>();
    *by_terms->mutable_field_id() = node.field_id();
    auto* options = by_terms->mutable_options();
    for (const auto& ngram : ngrams) {
      options->terms.emplace(ngram, kNoBoost);
    }
    by_terms->boost(node.Boost());
    slot = std::move(by_terms);
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
    by_phrase->boost(node.Boost());
    slot = std::move(by_phrase);
    return true;
  }
  return false;
}

void InitLoweringRules() {
  RegisterRule<WildcardSimplifyRule>();
  RegisterRule<RegexpSimplifyRule>();
  RegisterRule<EditDistanceSimplifyRule>();
  RegisterRule<PhraseLowerRule>();
  RegisterRule<PhraseSimplifyRule>();
  RegisterRule<NGramSimilarityLowerRule>();
}

namespace {

void LowerNode(Filter::ptr& slot) {
  const auto type = slot->type();
  if (type == Type<ByWildcard>::id()) {
    auto& node = sdb::basics::downCast<ByWildcard>(*slot);
    slot = LowerWildcard(node.field_id(), node.options().term,
                         node.options().scored_terms_limit, node.Boost());
  } else if (type == Type<ByRegexp>::id()) {
    auto& node = sdb::basics::downCast<ByRegexp>(*slot);
    slot = LowerRegexp(node.field_id(), node.options().pattern,
                       node.options().syntax, node.options().scored_terms_limit,
                       node.Boost());
  } else if (type == Type<ByEditDistance>::id()) {
    auto& node = sdb::basics::downCast<ByEditDistance>(*slot);
    slot = LowerLevenshtein(node.field_id(), node.options(), node.Boost());
  }
}

}  // namespace

void LowerAutomatons(Filter::ptr& root, const OptimizeContext& /*ctx*/) {
  if (root == nullptr) {
    return;
  }
  PostOrder(root, [](Filter::ptr& slot) { LowerNode(slot); });
}

}  // namespace irs::optimizer
