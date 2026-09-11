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

#include "iresearch/search/common/geo_of.hpp"
#include "iresearch/search/count/make.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/walk.hpp"
#include "iresearch/search/geo_query.hpp"

namespace irs::count {

template<typename Parser, typename Acceptor>
Root::ptr Make(const GeoQuery<Parser, Acceptor>& query, const Context& ctx) {
  SDB_ASSERT(query.Kind() != QueryKind::Empty);
  if (ctx.table != nullptr) {
    return search::MakeGeo<FilteredWalk, Root::ptr>(query, 0, ctx.table);
  }
  return search::MakeGeo<PlainWalk, Root::ptr>(query, 0, utils::Empty{});
}

}  // namespace irs::count
