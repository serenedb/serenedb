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
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/search/common/bitset_build.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/posting_fill.hpp"
#include "iresearch/search/fill/bitset_docs.hpp"
#include "iresearch/search/lead/bitset_docs.hpp"
#include "iresearch/search/probe/bitset_docs.hpp"

namespace irs::search {

inline constexpr bool kFoldOnlyWhenSmaller = false;

template<typename Result>
inline constexpr uint64_t kFoldPostings = 0;

template<>
inline constexpr uint64_t kFoldPostings<FillNode::ptr> = 4;
template<>
inline constexpr uint64_t kFoldPostings<LeadNode::ptr> = 4;

template<typename Terms>
uint64_t SumDocs(Terms terms) noexcept {
  uint64_t docs = 0;
  for (size_t i = 0; i != terms.size(); ++i) {
    docs += CookieOf(terms[i]).docs_count;
  }
  return docs;
}

template<typename Term>
uint64_t FoldReadClause(std::span<const Term> terms, doc_id_t docs_count,
                        uint64_t words) noexcept {
  uint64_t cost = 0;
  for (size_t i = 0; i != terms.size(); ++i) {
    cost += FoldRead(CookieOf(terms[i]).docs_count, docs_count, words);
  }
  return cost;
}

inline bool FoldIsFaster(uint64_t per_window, size_t terms, uint64_t docs,
                         doc_id_t docs_count) noexcept {
  const uint64_t windows = docs_count / kWindowDocs + 1;
  return terms * docs >= per_window * windows;
}

template<typename Leaf>
inline constexpr size_t kWindowLeafBytes =
  sizeof(Leaf) + sizeof(Leaf*) + sizeof(doc_id_t);

inline bool FoldIsSmaller(size_t terms, const IndexInput& doc,
                          doc_id_t docs_count) noexcept {
  const uint64_t leaf = ResolveInput(doc, []<typename Input> -> uint64_t {
    return kWindowLeafBytes<PostingFill<Input>>;
  });
  return terms * leaf >= (uint64_t{docs_count} + doc_limits::min()) / 8;
}

inline bool TakeFold(bool faster, size_t terms, const IndexInput& doc,
                     doc_id_t docs_count) noexcept {
  if (!faster) {
    return false;
  }
  if constexpr (kFoldOnlyWhenSmaller) {
    return FoldIsSmaller(terms, doc, docs_count);
  }
  return true;
}

template<typename Result, typename Terms>
bool TakeBitset(Terms terms, const IndexInput& doc, doc_id_t docs_count) {
  static_assert(kFoldPostings<Result> != 0,
                "this position has not said what a window costs it; a probed "
                "one is decided by TakeProbeBitset");
  if (terms.size() < 2) {
    return false;
  }
  const auto docs = SumDocs(terms);
  const auto faster =
    FoldIsFaster(kFoldPostings<Result>, terms.size(), docs, docs_count);
  return TakeFold(faster, terms.size(), doc, docs_count);
}

inline bool FoldProbeIsFaster(uint64_t interrogations, size_t terms,
                              uint64_t docs, doc_id_t docs_count) noexcept {
  return interrogations * terms >= docs + SegmentWords(docs_count);
}

template<typename Terms>
bool TakeProbeBitset(Terms terms, const IndexInput& doc, doc_id_t docs_count,
                     uint64_t interrogations) noexcept {
  if (terms.size() < 2) {
    return false;
  }
  const auto faster =
    FoldProbeIsFaster(interrogations, terms.size(), SumDocs(terms), docs_count);
  return TakeFold(faster, terms.size(), doc, docs_count);
}

inline uint64_t FoldConjunctionCost(
  std::span<const std::vector<PostingClause>> clauses, size_t lead, size_t seed,
  doc_id_t docs_count) noexcept {
  const auto words = SegmentWords(docs_count);
  uint64_t cost = words;
  for (size_t i = 0; i != clauses.size(); ++i) {
    const std::span<const PostingClause> clause{clauses[i]};
    if (i != lead) {
      cost += FoldReadClause(clause, docs_count, words);
    }
    if (i != seed) {
      cost += AppliedInPlace(clause, docs_count) ? words : 2 * words;
    }
  }
  return cost;
}

inline uint64_t WalkConjunctionCost(
  std::span<const std::vector<PostingClause>> clauses, size_t lead,
  uint64_t candidates) noexcept {
  uint64_t terms = 0;
  for (size_t i = 0; i != clauses.size(); ++i) {
    if (i != lead) {
      terms += clauses[i].size();
    }
  }
  return candidates * terms;
}

inline bool TakeConjunctionFold(const BitsetBuckets& buckets,
                                const IndexInput& doc, doc_id_t docs_count,
                                uint64_t candidates) noexcept {
  if (!buckets.NeedsSet()) {
    return false;
  }
  const std::span clauses{buckets.must};
  SDB_ASSERT(clauses.size() > 1);
  const auto cost =
    FoldConjunctionCost(clauses, 0, buckets.Seed(docs_count), docs_count);
  size_t terms = 0;
  for (const auto& clause : clauses) {
    terms += clause.size();
  }
  return TakeFold(WalkConjunctionCost(clauses, 0, candidates) >= cost, terms,
                  doc, docs_count);
}

template<typename Result>
Result MakeBitsetNode(BitsetBuckets&& buckets, const IndexInput& doc,
                      doc_id_t docs_count, TableFilter* table);

template<>
inline FillNode::ptr MakeBitsetNode<FillNode::ptr>(BitsetBuckets&& buckets,
                                                   const IndexInput& doc,
                                                   doc_id_t docs_count,
                                                   TableFilter*) {
  return memory::make_managed<fill::Impl<fill::BitsetDocs>>(
    BuildBitset(buckets, doc, docs_count));
}

template<>
inline ProbeNode::ptr MakeBitsetNode<ProbeNode::ptr>(BitsetBuckets&& buckets,
                                                     const IndexInput& doc,
                                                     doc_id_t docs_count,
                                                     TableFilter*) {
  return memory::make_managed<probe::Impl<probe::BitsetDocs>>(
    BuildBitset(buckets, doc, docs_count));
}

template<>
inline LeadNode::ptr MakeBitsetNode<LeadNode::ptr>(BitsetBuckets&& buckets,
                                                   const IndexInput& doc,
                                                   doc_id_t docs_count,
                                                   TableFilter*) {
  return memory::make_managed<lead::Impl<lead::BitsetDocs>>(
    BuildBitset(buckets, doc, docs_count));
}

template<typename Term>
BitsetBuckets DisjunctionBuckets(std::span<const Term> terms,
                                 const TermReader* field) {
  BitsetBuckets buckets;
  auto& clause = buckets.must.emplace_back();
  clause.reserve(terms.size());
  for (size_t i = 0; i != terms.size(); ++i) {
    clause.emplace_back(ClauseOf(terms[i], field));
  }
  return buckets;
}

template<typename Result, typename Term>
Result MakeBitsetOf(std::span<const Term> terms, const TermReader* field,
                    const IndexInput& doc, doc_id_t docs_count,
                    TableFilter* table) {
  if (!TakeBitset<Result>(terms, doc, docs_count)) {
    return {};
  }
  return MakeBitsetNode<Result>(DisjunctionBuckets(terms, field), doc,
                                docs_count, table);
}

template<typename Result, typename Term>
Result MakeBitsetWith(std::span<const Term> terms, const TermReader* field,
                      const IndexInput& doc, doc_id_t docs_count,
                      std::vector<FillNode::ptr>&& rest, TableFilter* table) {
  auto buckets = DisjunctionBuckets(terms, field);
  buckets.fills = std::move(rest);
  return MakeBitsetNode<Result>(std::move(buckets), doc, docs_count, table);
}

}  // namespace irs::search
