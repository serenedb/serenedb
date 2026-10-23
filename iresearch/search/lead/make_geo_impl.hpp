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

#include "iresearch/search/detail/geo_of.hpp"
#include "iresearch/search/detail/make_impl.hpp"
#include "iresearch/search/lead/family.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/queries/geo_query.hpp"

namespace irs::lead {

template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query) {
  return detail::MakeDocs<Family, detail::GeoKind>(query, {});
}

template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query,
               const detail::ScoredCtx& ctx) {
  return detail::MakeNode<Family, detail::GeoKind>(query, ctx);
}

}  // namespace irs::lead
