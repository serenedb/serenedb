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

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByPrefix;
struct FilterVisitor;

struct ByPrefixFilterOptions {
  bstring term;

  bool operator==(const ByPrefixFilterOptions& rhs) const noexcept {
    return term == rhs.term;
  }
};

struct ByPrefixOptions : ByPrefixFilterOptions {
  using FilterType = ByPrefix;
  using filter_options = ByPrefixFilterOptions;

  size_t scored_terms_limit{1024};

  bool operator==(const ByPrefixOptions& rhs) const noexcept {
    return filter_options::operator==(rhs) &&
           scored_terms_limit == rhs.scored_terms_limit;
  }
};

struct PrefixAcceptor {
  bytes_view prefix;

  bool operator()(bytes_view term) const noexcept {
    return term.starts_with(prefix);
  }
};

class ByPrefix : public FilterWithField<ByPrefixOptions> {
 public:
  static void visit(const SubReader& segment, const TermReader& reader,
                    const ByPrefixOptions& options, FilterVisitor& visitor);

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;
  static QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          const irs::field_id field,
                                          const bytes_view term);

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;

  TermPredicate::ptr CompileTermPredicate() const final;

  TermIterator::ptr CompileTermIterator(const TermReader& reader) const final;
};

}  // namespace irs
