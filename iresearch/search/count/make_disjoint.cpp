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

#include <absl/algorithm/container.h>

#include <array>
#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/count/disjoint.hpp"
#include "iresearch/search/count/make.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"

namespace irs::count {
namespace {

struct MemberClauses {
  uint32_t group = 0;
  bool exact = false;
};

bool DistinctSingles(std::span<const detail::PostingClause> clauses) {
  std::vector<uint64_t> singles;
  for (const auto& clause : clauses) {
    const auto& cookie = clause.state.cookie;
    if (clause.group == 0 && cookie.docs_count == 1) {
      const auto member =
        static_cast<uint64_t>(clause.state.reader->meta().dict.member);
      singles.push_back(member << 32 | cookie.doc_delta);
    }
  }
  absl::c_sort(singles);
  return absl::c_adjacent_find(singles) == singles.end();
}

bool NegationSums(const BooleanQuery& query) {
  return query.Terms(Occur::Must).empty() &&
         query.Queries(Occur::Must).empty() &&
         query.Bucket(Occur::Should).empty() &&
         query.Queries(Occur::MustNot).empty() &&
         !query.Terms(Occur::MustNot).empty() &&
         Disjoint(query.Terms(Occur::MustNot));
}

}  // namespace

bool Disjoint(std::span<const detail::PostingClause> clauses) {
  if (clauses.empty()) {
    return true;
  }
  const auto* first = clauses.front().state.reader;
  if (first == nullptr) {
    return false;
  }
  const auto& meta = first->meta();
  std::array<MemberClauses, kDictMembers> members{};
  bool several = false;
  for (const auto& clause : clauses) {
    const auto* reader = clause.state.reader;
    if (reader == nullptr || !reader->single_valued ||
        clause.stats.scorer != nullptr || reader->meta().id != meta.id ||
        reader->meta().dict.layout != meta.dict.layout) {
      return false;
    }
    several |= reader != first;
    auto& member = members[static_cast<size_t>(reader->meta().dict.member)];
    if (clause.group == 0) {
      if (member.group != 0) {
        return false;
      }
      member.exact = true;
    } else if (member.exact ||
               (member.group != 0 && member.group != clause.group)) {
      return false;
    } else {
      member.group = clause.group;
    }
  }
  if (several && IsList(meta.dict.layout)) {
    return false;
  }
  return DistinctSingles(clauses);
}

uint64_t SumOf(std::span<const detail::PostingClause> clauses) noexcept {
  uint64_t sum = 0;
  for (const auto& clause : clauses) {
    sum += clause.state.cookie.docs_count;
  }
  return sum;
}

bool ShouldSums(const BooleanQuery& query, const Context& ctx) {
  return ctx.table == nullptr && query.MinShouldMatch() == 1 &&
         query.Bucket(Occur::Must).empty() &&
         query.Bucket(Occur::MustNot).empty() &&
         query.Queries(Occur::Should).empty() &&
         query.Bucket(Occur::Should).all_docs.empty() &&
         !query.Terms(Occur::Should).empty() &&
         Disjoint(query.Terms(Occur::Should));
}

bool MustMisses(const BooleanQuery& query) {
  const auto terms = query.Terms(Occur::Must);
  return terms.size() >= 2 && Disjoint(terms);
}

bool IsConstant(const QueryBuilder& query) {
  switch (query.Kind()) {
    case QueryKind::Empty:
    case QueryKind::All:
    case QueryKind::Term:
      return true;
    case QueryKind::Terms: {
      const auto* reader =
        irs::utils::downCast<MultiTermQuery>(query).State().Reader();
      return reader != nullptr && reader->single_valued;
    }
    case QueryKind::Boolean: {
      const auto& boolean = irs::utils::downCast<BooleanQuery>(query);
      return ShouldSums(boolean, {}) || MustMisses(boolean) ||
             NegationSums(boolean);
    }
    default:
      return false;
  }
}

}  // namespace irs::count
