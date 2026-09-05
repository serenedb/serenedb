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

#include <concepts>
#include <cstdint>
#include <limits>
#include <span>

#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/node.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/node.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/node.hpp"
#include "iresearch/search/states/term_state.hpp"

namespace irs {

class TermQuery;
class MultiTermQuery;
class FixedPhraseQuery;
class VariadicPhraseQuery;
class AllQuery;
class NGramSimilarityQuery;
class WildcardNgramQuery;
class ByNestedQuery;
class KnnVectorQuery;
class RangeVectorQuery;
class BooleanQuery;
struct EmptyQueryBuilder;

template<typename Parser, typename Acceptor>
class GeoQuery;

namespace search {

using LeadNode = lead::Node;
using ProbeNode = probe::Node;
using FillNode = fill::Node;

template<typename Term>
const PostingMeta& CookieOf(const Term& term) noexcept {
  if constexpr (std::same_as<Term, PostingClause>) {
    return term.state.cookie;
  } else {
    return term.cookie;
  }
}

template<typename Term>
PostingClause ClauseOf(const Term& term, const TermReader* field,
                       const Scorer* scorer = nullptr,
                       score_t boost = kNoBoost) noexcept {
  if constexpr (std::same_as<Term, PostingClause>) {
    return term;
  } else {
    PostingClause out{TermState{field, term.cookie}};
    if (scorer != nullptr) {
      if (term.stats != nullptr) {
        out.stats = {term.stats, scorer};
      }
      out.boost = term.boost * boost;
    }
    return out;
  }
}

template<typename Term>
const TermReader& FieldOf(const Term& term, const TermReader* field) noexcept {
  if constexpr (std::same_as<Term, PostingClause>) {
    return *term.state.reader;
  } else {
    return *field;
  }
}

template<typename Term, typename TermCb, typename QueryCb>
bool VisitOrderedOf(std::span<const Term> terms,
                    std::span<const QueryBuilder::ptr> filters, bool ascending,
                    size_t skip, size_t limit, TermCb&& term_cb,
                    QueryCb&& query_cb) {
  size_t t = 0;
  size_t q = 0;
  size_t at = 0;
  size_t emitted = 0;
  while ((t != terms.size() || q != filters.size()) && emitted != limit) {
    const bool take_term =
      q == filters.size() ||
      (t != terms.size() &&
       (ascending
          ? CookieOf(terms[t]).docs_count <= filters[q]->EstimateMax()
          : CookieOf(terms[t]).docs_count >= filters[q]->EstimateMax()));
    if (at++ < skip) {
      t += static_cast<size_t>(take_term);
      q += static_cast<size_t>(!take_term);
      continue;
    }
    ++emitted;
    if (take_term ? !term_cb(terms[t++]) : !query_cb(*filters[q++])) {
      return false;
    }
  }
  return true;
}

template<typename Term>
bool HeadIsTerm(std::span<const Term> terms,
                std::span<const QueryBuilder::ptr> filters) noexcept {
  if (filters.empty()) {
    return true;
  }
  if (terms.empty()) {
    return false;
  }
  return CookieOf(terms.front()).docs_count <= filters.front()->EstimateMax();
}

template<typename Term>
uint64_t HeadEstimate(std::span<const Term> terms,
                      std::span<const QueryBuilder::ptr> filters) noexcept {
  return HeadIsTerm(terms, filters) ? CookieOf(terms.front()).docs_count
                                    : filters.front()->EstimateMax();
}

inline Terms UniformityOf(const TermReader& field,
                          const Scorer* scorer) noexcept {
  if (!FreqOf(field) || !ScoresPerDoc(scorer)) {
    return Terms::Constant;
  }
  if (BoundsOf(field) && HasScoreBounds(scorer)) {
    return Terms::Bounded;
  }
  return Terms::Scored;
}

inline constexpr uint32_t kBitplaneMaxMatch = 2;

inline constexpr uint64_t kDensityThresholdInverse = 32;

inline bool SubtractsConjunction(doc_id_t rarest,
                                 doc_id_t docs_count) noexcept {
  return rarest >= docs_count / kDensityThresholdInverse;
}

inline bool SubtractsDisjunction(doc_id_t rarest, doc_id_t densest) noexcept {
  constexpr uint64_t kSkew = 256;
  return uint64_t{rarest} * kSkew <= densest;
}

}  // namespace search
}  // namespace irs
