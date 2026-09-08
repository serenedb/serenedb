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

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/probe_leaves.hpp"
#include "iresearch/search/probe/make.hpp"

namespace irs::probe {
namespace {

Node::ptr BitsetOfTerms(std::span<const search::PostingClause> terms,
                        const IndexInput& doc, doc_id_t docs_count,
                        uint64_t interrogations) {
  if (!search::TakeProbeBitset(terms, doc, docs_count, interrogations)) {
    return {};
  }
  return search::MakeBitsetNode<Node::ptr>(
    search::DisjunctionBuckets(terms, nullptr), doc, docs_count, nullptr);
}

}  // namespace

Node::ptr MakeBitsetDisjunctionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  uint64_t interrogations) {
  const IndexInput* doc = nullptr;
  if (!search::ConcreteClauses(terms, filters, nullptr, doc)) {
    return {};
  }
  return BitsetOfTerms(terms, *doc, static_cast<doc_id_t>(segment.docs_count()),
                       interrogations);
}

}  // namespace irs::probe
