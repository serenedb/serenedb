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

#include <algorithm>
#include <cstdint>
#include <span>
#include <utility>

#include "iresearch/search/count/root.hpp"
#include "iresearch/search/detail/bitset_build.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::count {

class Subtract : public Root {
 public:
  Subtract(Root::ptr total, Root::ptr excluded, bool partial) noexcept
    : _total{std::move(total)},
      _excluded{std::move(excluded)},
      _partial{partial} {}

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    const auto total = _total->Run(min, max);
    const auto excluded = _excluded->Run(min, max);
    SDB_ASSERT(_partial || excluded <= total);
    return total - excluded;
  }

  uint64_t Finish() final { return _total->Finish() - _excluded->Finish(); }

 private:
  Root::ptr _total;
  Root::ptr _excluded;
  bool _partial;
};

Root::ptr MakeSubtractConjunction(std::span<const detail::PostingClause> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  const SubReader& segment, const Context& ctx);
Root::ptr MakeSubtractDisjunction(const detail::PostingClause& first,
                                  const detail::PostingClause& second,
                                  const SubReader& segment, const Context& ctx);

template<typename Term>
doc_id_t RarestOf(std::span<const Term> terms) noexcept {
  SDB_ASSERT(terms.size() == 2);
  return std::min(detail::CookieOf(terms.front()).docs_count,
                  detail::CookieOf(terms.back()).docs_count);
}

template<typename Term>
doc_id_t DensestOf(std::span<const Term> terms) noexcept {
  SDB_ASSERT(terms.size() == 2);
  return std::max(detail::CookieOf(terms.front()).docs_count,
                  detail::CookieOf(terms.back()).docs_count);
}

template<typename Term>
bool SubtractsPair(std::span<const Term> terms, doc_id_t docs_count) noexcept {
  if (terms.size() != 2) {
    return false;
  }
  return detail::ProbeBeatsFold(RarestOf(terms), DensestOf(terms), docs_count);
}

}  // namespace irs::count
