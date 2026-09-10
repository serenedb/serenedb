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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/common/scored_node_builder.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/fill/make_boolean.hpp"

namespace irs::fill {

Node::ptr Make(const BooleanQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge) {
  return search::builder::MakeNode<ScoredApi>(query, ctx, merge);
}

}  // namespace irs::fill
