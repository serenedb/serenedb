////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "view.h"

#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/graph_view.h"
#include "catalog/sql_query_view.h"
#include "general_server/state.h"
#include "vpack/serializer.h"

#ifdef SDB_CLUSTER
#include "search/search_view.h"
#endif

namespace sdb::catalog {

ViewMeta ViewMeta::Make(const View& view) {
  return {
    .id = Identifier{view.GetId().id()},
    .name = std::string{view.GetName()},
    .type = view.GetViewType(),
  };
}

Result ViewOptions::Read(ViewOptions& options, vpack::Slice slice) {
  auto r = vpack::ReadObjectNothrow(slice, options.meta,
                                    {.skip_unknown = true, .strict = true});
  if (!r.ok()) {
    return r;
  }

  options.properties = slice;
  return {};
}

View::View(ViewMeta&& options, ObjectId database_id)
  : SchemaObject{{},
                 database_id,
                 *options.id,
                 std::move(options.name),
                 ObjectType::View},
    _type{options.type} {}

Result CreateViewInstance(std::shared_ptr<catalog::View>& view,
                          ObjectId database_id, ViewOptions&& options,
                          ViewContext ctx) {
  SDB_ASSERT(ServerState::instance()->IsClientNode());

  switch (options.meta.type) {
    case ViewType::ViewSqlQuery:
      return SqlQueryView::Make(view, database_id, std::move(options), ctx);
    case ViewType::ViewGraph:
      return GraphView::Make(view, database_id, std::move(options),
                             ctx != ViewContext::Internal);
    case ViewType::ViewSearch:
#ifdef SDB_CLUSTER
      return SearchView::Make(view, database_id, std::move(options),
                              ctx != ViewContext::Internal);
#endif
  }
  return {};
}

}  // namespace sdb::catalog
