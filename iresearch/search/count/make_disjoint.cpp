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
#include <compare>
#include <cstdint>
#include <span>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/count/disjoint.hpp"
#include "iresearch/search/count/make.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/utils/containers/small_vector.hpp"

namespace irs::count {
namespace {

struct MemberClauses {
  uint32_t group = 0;
  bool exact = false;
};

struct TermKey {
  enum class Kind : uint8_t {
    Single,
    Inline,
    Stored,
  };

  DictMember member;
  Kind kind;
  uint64_t at;

  auto operator<=>(const TermKey&) const = default;
};

TermKey KeyOf(const detail::PostingClause& clause) noexcept {
  const auto& cookie = clause.state.cookie;
  const auto member = clause.state.reader->meta().dict.member;
  if (cookie.docs_count == 1) {
    return {
      .member = member, .kind = TermKey::Kind::Single, .at = cookie.doc_delta};
  }
  if (cookie.inline_size != 0) {
    return {.member = member,
            .kind = TermKey::Kind::Inline,
            .at = reinterpret_cast<uintptr_t>(cookie.inline_data)};
  }
  return {
    .member = member, .kind = TermKey::Kind::Stored, .at = cookie.doc_start};
}

bool DistinctTerms(std::span<const detail::PostingClause> clauses) {
  if (clauses.size() < 2) {
    return true;
  }
  containers::SmallVector<TermKey, 16> keys;
  for (const auto& clause : clauses) {
    if (clause.group == 0) {
      keys.push_back(KeyOf(clause));
    }
  }
  absl::c_sort(keys);
  return absl::c_adjacent_find(keys) == keys.end();
}

bool NegationSums(const BooleanQuery& query) {
  return query.Terms(Occur::Must).empty() &&
         query.Queries(Occur::Must).empty() &&
         query.Bucket(Occur::Should).empty() &&
         query.Queries(Occur::MustNot).empty() &&
         !query.Terms(Occur::MustNot).empty() &&
         query.Bucket(Occur::MustNot).disjoint;
}

}  // namespace

bool Disjoint(std::span<const detail::PostingClause> clauses) {
  if (clauses.empty()) {
    return true;
  }
  const auto* first = clauses.front().state.reader;
  SDB_ASSERT(first);
  const auto& meta = first->meta();
  std::array<MemberClauses, kDictMembers> members{};
  bool several = false;
  for (const auto& clause : clauses) {
    const auto* reader = clause.state.reader;
    SDB_ASSERT(reader);
    if (!reader->single_valued || reader->meta().id != meta.id ||
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
  return DistinctTerms(clauses);
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
         query.Bucket(Occur::Should).disjoint;
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
      SDB_ASSERT(reader);
      return reader->single_valued;
    }
    case QueryKind::Boolean: {
      const auto& boolean = irs::utils::downCast<BooleanQuery>(query);
      return ShouldSums(boolean, {}) || NegationSums(boolean);
    }
    default:
      return false;
  }
}

}  // namespace irs::count
