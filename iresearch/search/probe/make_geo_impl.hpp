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

#include "iresearch/search/detail/geo_of.hpp"
#include "iresearch/search/detail/make_impl.hpp"
#include "iresearch/search/probe/family.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/queries/geo_query.hpp"

namespace irs::probe {

template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query,
               uint64_t interrogations) {
  return detail::MakeDocs<Family, detail::GeoKind>(
    query, {.interrogations = interrogations});
}

template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query,
               const detail::ScoredCtx& ctx, uint64_t interrogations) {
  return detail::MakeNode<Family, detail::GeoKind>(
    query, {.scored = ctx, .interrogations = interrogations});
}

}  // namespace irs::probe
