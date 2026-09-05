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

#include <cstdint>
#include <utility>

#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/geo_of.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/geo_query.hpp"
#include "iresearch/search/lead/constant_scored.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"

namespace irs::lead {

template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query) {
  return search::MakeGeo<Impl, Node::ptr>(query, 0);
}

template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query, const ScoredCtx& ctx) {
  auto node = Make(query);
  if (!node) {
    return {};
  }
  const auto record = query.Stats(ctx);
  const auto value =
    search::AllDocsScore(query.Segment(), ScoreArgs{.scorer = record.scorer,
                                                    .stats = record.stats,
                                                    .fetcher = ctx.fetcher,
                                                    .boost = query.Boost()});
  using Node = ConstantScored<Erased>;
  return memory::make_managed<Impl<Node>>(value, std::move(node));
}

}  // namespace irs::lead
