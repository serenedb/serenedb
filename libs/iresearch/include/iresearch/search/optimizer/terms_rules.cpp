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

#include "iresearch/search/optimizer/terms_rules.hpp"

#include <memory>
#include <vector>

#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/optimizer/common.hpp"
#include "iresearch/search/term_filter.hpp"

namespace irs::optimizer {

bool ByTermsMinMatchZeroRule::Apply(Filter::ptr& slot,
                                    const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<ByTerms>(*slot);
  if (node.options().terms.empty() || node.options().min_match != 0) {
    return false;
  }
  if (ctx.scorer == nullptr) {
    slot = node.MakeAllDocsFilter(kNoBoost);
    return true;
  }
  auto terms = std::make_unique<ByTerms>();
  terms->boost(node.Boost());
  *terms->mutable_field_id() = node.field_id();
  *terms->mutable_options() = node.options();
  terms->mutable_options()->min_match = 1;

  std::vector<Filter::ptr> children;
  children.emplace_back(node.MakeAllDocsFilter(0.F));
  children.emplace_back(std::move(terms));
  slot = MakeBoolean<Or>(std::move(children), ScoreMergeType::Sum);
  return true;
}

bool ByTermsDegenerateRule::Apply(Filter::ptr& slot,
                                  const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<ByTerms>(*slot);
  const auto& terms = node.options().terms;
  const auto min_match = node.options().min_match;
  if (terms.empty()) {
    slot = std::make_unique<Empty>();
    return true;
  }
  if (min_match == 0) {
    return false;
  }
  if (min_match > terms.size()) {
    slot = std::make_unique<Empty>();
    return true;
  }
  if (terms.size() != 1) {
    return false;
  }
  const auto& term = *terms.begin();
  auto by_term = std::make_unique<ByTerm>();
  *by_term->mutable_field_id() = node.field_id();
  by_term->mutable_options()->term = term.term;
  by_term->boost(node.Boost() * term.boost);
  slot = std::move(by_term);
  return true;
}

}  // namespace irs::optimizer
