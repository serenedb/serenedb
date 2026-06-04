////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "basics/result.h"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/type_id.hpp"

namespace sdb {

enum class Conjunction { Or, And };
enum class Modifier { None, Required, Not };

struct ParserContext {
  struct Group {
    std::vector<irs::Filter::ptr> req;
    std::vector<irs::Filter::ptr> opt;
  };

  std::string_view default_field;
  irs::MixedBooleanFilter* out;
  irs::analysis::Analyzer* tokenizer;
  std::vector<Group> stack;
  std::string error_message;
  Modifier last_mod{Modifier::None};
  bool strict_field = false;

  ParserContext(irs::MixedBooleanFilter& out, std::string_view field,
                irs::analysis::Analyzer& tokenizer)
    : default_field(field), out{&out}, tokenizer{&tokenizer} {
    stack.emplace_back();
  }

  Group& Current() noexcept { return stack.back(); }

  void PushGroup() { stack.emplace_back(); }

  irs::FilterWithBoost* PopGroupAsClause() {
    Group group = std::move(stack.back());
    stack.pop_back();
    auto mixed = std::make_unique<irs::MixedBooleanFilter>(
      std::move(group.req), std::move(group.opt));
    auto& ref = *mixed;
    Current().opt.emplace_back(std::move(mixed));
    return &ref;
  }

  void Finalize() {
    auto& group = stack.front();
    *out = irs::MixedBooleanFilter{std::move(group.req), std::move(group.opt)};
  }

  void AddClause(Conjunction conj) {
    if (conj != Conjunction::And && last_mod == Modifier::None) {
      return;
    }

    auto& opt = Current().opt;
    if (opt.empty()) {
      return;
    }

    auto current_clause = std::move(opt.back());
    opt.pop_back();

    if (conj == Conjunction::And && !opt.empty()) {
      Current().req.emplace_back(std::move(opt.back()));
      opt.pop_back();
    }

    if (last_mod == Modifier::Not) {
      std::vector<irs::Filter::ptr> target;
      target.emplace_back(std::move(current_clause));
      Current().req.emplace_back(std::make_unique<irs::Not>(
        std::make_unique<irs::Or>(std::move(target))));
    } else {
      Current().req.emplace_back(std::move(current_clause));
    }
  }

  irs::ByTerm& AddTerm(std::string_view value) {
    auto& f = Emplace<irs::ByTerm>();
    *f.mutable_field() = default_field;
    f.mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
    return f;
  }

  irs::ByPhrase& AddPhrase(std::string_view value, int slop = 0) {
    auto& f = Emplace<irs::ByPhrase>();
    *f.mutable_field() = default_field;
    tokenizer->reset(value);
    auto token = irs::get<irs::TermAttr>(*tokenizer);
    for (; tokenizer->next();) {
      f.mutable_options()->push_back<irs::ByTermOptions>().term = token->value;
    }
    return f;
  }

  irs::ByPrefix& AddPrefix(std::string_view value) {
    auto& f = Emplace<irs::ByPrefix>();
    *f.mutable_field() = default_field;
    if (!value.empty() && value.back() == '*') {
      value.remove_suffix(1);
    }
    f.mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
    return f;
  }

  irs::ByWildcard& AddWildcard(std::string_view value) {
    auto& f = Emplace<irs::ByWildcard>();
    *f.mutable_field() = default_field;
    f.mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
    return f;
  }

  irs::ByRange& AddRange(std::string_view min_val, std::string_view max_val,
                         bool inc_min, bool inc_max) {
    auto& f = Emplace<irs::ByRange>();
    *f.mutable_field() = default_field;
    auto& range = f.mutable_options()->range;
    if (min_val == "*") {
      range.min_type = irs::BoundType::Unbounded;
    } else {
      range.min = irs::ViewCast<irs::byte_type>(min_val);
      range.min_type =
        inc_min ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
    if (max_val == "*") {
      range.max_type = irs::BoundType::Unbounded;
    } else {
      range.max = irs::ViewCast<irs::byte_type>(max_val);
      range.max_type =
        inc_max ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
    return f;
  }

  irs::ByEditDistance& AddFuzzy(std::string_view value, int distance) {
    auto& f = Emplace<irs::ByEditDistance>();
    *f.mutable_field() = default_field;
    f.mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
    f.mutable_options()->max_distance = static_cast<uint8_t>(distance);
    return f;
  }

 private:
  template<typename T>
  T& Emplace() {
    auto filter = std::make_unique<T>();
    auto& ref = *filter;
    Current().opt.emplace_back(std::move(filter));
    return ref;
  }
};

Result ParseQuery(ParserContext& ctx, std::string_view input);

}  // namespace sdb
