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

#include <cstdint>
#include <span>

#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/with_mask.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/make_boolean.hpp"

namespace irs::lead {

Node::ptr Api::MakeNegation(
  std::span<const detail::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx) {
  if (detail::OnlyMask(exclude_terms, exclude_filters)) {
    return MakeLiveDocs(segment);
  }
  return detail::builder::MakeSparseNegation<Api>(
    exclude_terms, exclude_filters, segment, candidates, ctx);
}

}  // namespace irs::lead
