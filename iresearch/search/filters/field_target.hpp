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

#pragma once

#include <memory>

#include "iresearch/search/filters/automaton_filter.hpp"
#include "iresearch/search/filters/prefix_filter.hpp"
#include "iresearch/search/filters/term_filter.hpp"

namespace irs {

struct FieldTarget {
  field_id id;
  DictRef dict;
  score_t boost;
  const Scorer* scorer = nullptr;

  template<typename Filter>
  std::unique_ptr<Filter> Make() const {
    auto filter = std::make_unique<Filter>();
    *filter->mutable_field_id() = id;
    *filter->mutable_dict() = dict;
    filter->SetBoost(boost);
    if (scorer != nullptr) {
      filter->SetScorer(scorer);
    }
    return filter;
  }

  Filter::ptr Term(bytes_view term) const {
    auto filter = Make<ByTerm>();
    filter->mutable_options()->term = term;
    return filter;
  }

  Filter::ptr Prefix(bytes_view prefix) const {
    auto filter = Make<ByPrefix>();
    filter->mutable_options()->term = prefix;
    return filter;
  }

  Filter::ptr Automaton(bytes_view pattern, PatternKind kind) const {
    auto filter = Make<AutomatonFilter>();
    *filter->mutable_options() = AutomatonOptions{pattern, kind};
    return filter;
  }
};

template<typename Node>
FieldTarget TargetOf(const Node& node) noexcept {
  return {
    .id = node.field_id(),
    .dict = node.dict(),
    .boost = node.GetBoost(),
    .scorer = node.GetScorer(),
  };
}

}  // namespace irs
