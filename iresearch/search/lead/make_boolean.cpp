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

#include "iresearch/search/lead/make_boolean.hpp"

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/queries/boolean_query.hpp"

namespace irs::lead {

Node::ptr MakeRequiredDocs(std::span<const detail::PostingClause> must,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const detail::PostingClause> should,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match,
                           const SubReader& segment) {
  return detail::builder::MakeRequired<Api>(
    must, must_filters, should, should_filters, min_should_match, segment, {});
}

Node::ptr Make(const BooleanQuery& query) {
  return detail::builder::Make<Api>(query, {});
}

}  // namespace irs::lead
