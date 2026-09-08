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
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/window_of.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/lead/window_conjunction_docs.hpp"

namespace irs::lead {

Node::ptr MakeWindowConjunctionDocs(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const SubReader& segment) {
  if (terms.size() + filters.size() < 2) {
    return {};
  }
  const IndexInput* doc = nullptr;
  if (!search::WindowTerms(terms, filters, nullptr, doc)) {
    return {};
  }
  if (!search::DenseConjunction(terms,
                                static_cast<doc_id_t>(segment.docs_count()))) {
    return {};
  }
  return ResolveInput(*doc, [&]<typename Input> -> Node::ptr {
    using Leaf = PostingFill<Input>;
    using Others = fill::AndLeaves<Leaf>;
    const auto& lead = terms.front();
    const auto& lead_own = FieldOf(lead, nullptr);
    const auto& lead_meta = CookieOf(lead);
    const auto others = [&](Leaf& leaf, size_t i) {
      const auto& term = terms[i + 1];
      const auto& own = FieldOf(term, nullptr);
      const auto& meta = CookieOf(term);
      leaf.Prepare(meta, *doc, meta.docs_count != 1 && search::BoundsOf(own),
                   meta.docs_count != 1 && search::FreqOf(own));
    };
    using Node = WindowConjunctionDocs<Leaf, Others>;
    return memory::make_managed<Impl<Node>>(
      std::piecewise_construct,
      std::forward_as_tuple(
        lead_meta, *doc,
        lead_meta.docs_count != 1 && search::BoundsOf(lead_own),
        lead_meta.docs_count != 1 && search::FreqOf(lead_own)),
      std::forward_as_tuple(terms.size() - 1, others));
  });
}

}  // namespace irs::lead
