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
#include <cmath>
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

inline constexpr double kSparseProbeCost = 0.3;
inline constexpr double kLeafProbeCost = 1.0;
inline constexpr double kErasedProbeCost = 1.2;
inline constexpr double kMultiProbeCost = 16.0;
inline constexpr double kBlockProbeCost = 10.0;
inline constexpr double kBlockSeekCost = 1000.0;
inline constexpr double kBlockChildProbeCost = 60.0;
inline constexpr double kBlockPhraseProbeCost = 200.0;
inline constexpr double kStepBaseCost = 8.0;
inline constexpr double kStepJumpCost = 24.0;
inline constexpr double kDecodeCost = 0.25;
inline constexpr double kPositionalMatchCost = 110.0;
inline constexpr double kPositionalLeastCost = 8.0;
inline constexpr double kBooleanHitCost = 32.0;
inline constexpr double kBooleanFillWeight = 1.0;
inline constexpr double kHitCost = 1.5;
inline constexpr double kWindowRefillCost = 100.0;
inline constexpr double kWindowChildRefillCost = 60.0;
inline constexpr double kWindowChildSeekCost = 250.0;
inline constexpr double kWindowNestedRefillCost = 800.0;
inline constexpr double kEagerNestedWindowCost = 300.0;
inline constexpr double kProbedLeadCost = 2.0;
inline constexpr double kFilledCost = 1.0;
inline constexpr double kClearCost = 0.9;
inline constexpr double kEmitWordCost = 6.0;
inline constexpr double kLazyFillCost = 5.0;
inline constexpr double kWindowTestCost = 1.2;
inline constexpr double kWindowBlockCost = 2.0;
inline constexpr double kWindowDirtyTestCost = 2.0;
inline constexpr double kBitClearCost = 0.25;
inline constexpr double kBitTestCost = 1.0;
inline constexpr double kBitsetBlockCost = 100.0;
inline constexpr double kBitsetBlockWordCost = 1.0;
inline constexpr double kBitsetDirtyTestCost = 0.5;
inline constexpr double kEagerChildWindowCost = 50.0;
inline constexpr double kWindowLeadFillCost = 0.5;
inline constexpr double kWindowLeadCost = 200.0;
inline constexpr double kWindowChildLeadCost = 50.0;
inline constexpr double kBitsetProbeCost = 1.5;
inline constexpr double kBuildCost = 0.6;
inline constexpr double kSeedBuildCost = 0.65;
inline constexpr double kDenseBuildWordCost = 3.0;
inline constexpr double kFoldWordCost = 0.25;
inline constexpr uint64_t kPhraseMatchShare = 16;

enum class ExcludeUse { PerDoc, PerBlock };

struct ClauseCost {
  uint64_t docs;
  double matches;
  double sparse;
  double fill;
  double lazy_fill;
  double probe;
  double block_probe;
  double hit;
  uint64_t leaves;
  bool exact;
  bool nested;
};

inline double PostingBuildCost(uint64_t docs, doc_id_t docs_count,
                               double per_doc) noexcept {
  const auto d = static_cast<double>(std::max<uint64_t>(docs, 1));
  const auto words = static_cast<double>(SegmentWords(docs_count));
  return std::min(per_doc * d, kDenseBuildWordCost * words *
                                 static_cast<double>(docs_count) / d);
}

inline double DocsEmitCost(double docs, double words) noexcept {
  return kEmitWordCost * words * (1.0 - std::exp(-docs / words));
}

inline ClauseCost TermClauseCost(uint64_t docs, doc_id_t docs_count) noexcept {
  const auto d = static_cast<double>(docs);
  return {.docs = docs,
          .matches = d,
          .sparse = d,
          .fill = PostingBuildCost(docs, docs_count, kBuildCost),
          .lazy_fill = kLazyFillCost * d,
          .probe = kSparseProbeCost,
          .block_probe = kBlockProbeCost,
          .hit = kHitCost,
          .leaves = 1,
          .exact = true,
          .nested = false};
}

inline ClauseCost ChildClauseCost(const QueryBuilder& child,
                                  doc_id_t docs_count) noexcept {
  const uint64_t docs = child.EstimateMax();
  const auto d = static_cast<double>(docs);
  const uint64_t leaves = std::max<uint32_t>(child.Leaves(), 1);
  const auto l = static_cast<double>(leaves);
  switch (child.Kind()) {
    case QueryKind::Term:
    case QueryKind::All:
      return TermClauseCost(docs, docs_count);
    case QueryKind::Terms:
      return {.docs = docs,
              .matches = d,
              .sparse = d,
              .fill = PostingBuildCost(docs, docs_count, kBuildCost),
              .lazy_fill = kLazyFillCost * d,
              .probe = kSparseProbeCost + kMultiProbeCost * l,
              .block_probe = kMultiProbeCost * l,
              .hit = kHitCost,
              .leaves = leaves,
              .exact = true,
              .nested = false};
    case QueryKind::Phrase: {
      const auto matches = static_cast<double>(child.EstimateMatches());
      const auto fill =
        kPositionalMatchCost * matches + kPositionalLeastCost * d;
      return {.docs = docs,
              .matches = matches,
              .sparse = matches / static_cast<double>(kPhraseMatchShare),
              .fill = fill,
              .lazy_fill = fill,
              .probe = kSparseProbeCost + kErasedProbeCost * l,
              .block_probe = kBlockPhraseProbeCost,
              .hit = kPositionalMatchCost,
              .leaves = leaves,
              .exact = false,
              .nested = false};
    }
    default: {
      const auto postings =
        kBooleanFillWeight * static_cast<double>(child.Postings());
      return {.docs = docs,
              .matches = d,
              .sparse = d,
              .fill = postings,
              .lazy_fill = postings,
              .probe = kSparseProbeCost + kErasedProbeCost * l,
              .block_probe = kBlockChildProbeCost,
              .hit = kBooleanHitCost * std::max(l - 1.0, 1.0),
              .leaves = leaves,
              .exact = false,
              .nested = true};
    }
  }
}

struct FoldEmit {
  double word;
  double sparse_lead;
  bool docs;
};

template<typename Result>
inline constexpr uint64_t kFoldPostings = 0;

template<>
inline constexpr uint64_t kFoldPostings<FillNode::ptr> = 4;
template<>
inline constexpr uint64_t kFoldPostings<LeadNode::ptr> = 4;

template<typename Result>
inline constexpr FoldEmit kFoldEmit{0.0, 1.0, true};

template<>
inline constexpr FoldEmit kFoldEmit<FillNode::ptr>{0.5, 6.0, false};

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

}  // namespace irs::search
