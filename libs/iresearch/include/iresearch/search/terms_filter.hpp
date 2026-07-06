////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <set>
#include <utility>

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/all_docs_provider.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByTerms;
struct FilterVisitor;

/// Options for terms filter
struct ByTermsOptions {
  struct SearchTerm {
    bstring term;
    score_t boost;

    SearchTerm() = default;

    explicit SearchTerm(bstring&& term, score_t boost = kNoBoost) noexcept
      : term(std::move(term)), boost(boost) {}

    explicit SearchTerm(bytes_view term, score_t boost = kNoBoost)
      : term(term.data(), term.size()), boost(boost) {}

    bool operator==(const SearchTerm& rhs) const noexcept {
      return term == rhs.term && boost == rhs.boost;
    }

    bool operator<(const SearchTerm& rhs) const noexcept {
      return term < rhs.term;
    }

    bool operator<(bytes_view rhs) const noexcept { return term < rhs; }

    friend bool operator<(bytes_view lhs, const SearchTerm& rhs) noexcept {
      return lhs < rhs.term;
    }
  };

  using FilterType = ByTerms;
  using search_terms = std::set<SearchTerm, std::less<>>;

  search_terms terms;
  size_t min_match{1};
  ScoreMergeType merge_type{ScoreMergeType::Sum};

  bool operator==(const ByTermsOptions& rhs) const noexcept {
    return min_match == rhs.min_match && merge_type == rhs.merge_type &&
           terms == rhs.terms;
  }
};

struct TermSetAcceptor {
  const ByTermsOptions::search_terms* terms;

  bool operator()(bytes_view term) const { return terms->contains(term); }
};

// Filter by a set of terms
class ByTerms final : public FilterWithField<ByTermsOptions>,
                      public AllDocsProvider {
 public:
  static void visit(const SubReader& segment, const TermReader& field,
                    const ByTermsOptions& options, FilterVisitor& visitor);

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;
  static QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          irs::field_id field,
                                          const ByTermsOptions& options,
                                          score_t boost);

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;

  TermPredicate::ptr CompileTermPredicate() const final;

  TermIterator::ptr CompileTermIterator(const TermReader& reader) const final;
};

}  // namespace irs
