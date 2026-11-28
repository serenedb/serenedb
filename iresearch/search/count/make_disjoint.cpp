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
#include <absl/hash/hash.h>

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

bool ShouldSums(const BooleanQuery& query, const Context& ctx) {
  return !ctx.table && query.MinShouldMatch() == 1 &&
         query.Bucket(Occur::Must).empty() &&
         query.Bucket(Occur::MustNot).empty() &&
         query.Queries(Occur::Should).empty() &&
         query.Bucket(Occur::Should).all_docs.empty() &&
         !query.Terms(Occur::Should).empty() &&
         query.Bucket(Occur::Should).disjoint;
}

}  // namespace irs::count
