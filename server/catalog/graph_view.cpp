////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "graph_view.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cstddef>
#include <magic_enum/magic_enum.hpp>
#include <type_traits>
#include <vector>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/result.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/identifiers/identifier.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "catalog/types.h"
#include "catalog/view.h"
#include "general_server/state.h"
#include "vpack/serializer.h"

namespace sdb::catalog {
namespace {

template<typename T>
struct Update {
  vpack::Optional<std::vector<T>> edges;
};

struct UserEntryImpl {
  std::optional<EdgeDirection> direction;
  std::string_view collection;
};

struct UserEntry : UserEntryImpl {};

template<typename Context>
void VPackRead(Context ctx, UserEntry& v) {  // NOLINT
  static_assert(std::is_same_v<vpack::ObjectFormat, typename Context::Format>);

  if (auto slice = ctx.vpack(); slice.isString()) {
    v.collection = slice.stringView();
  } else {
    vpack::ReadObject<UserEntryImpl>(slice, v);
  }
}

auto MakeCollectionChecker(ObjectId database) {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  return [&, database](std::string_view edge,
                       std::shared_ptr<catalog::Table>& collection) -> Result {
    collection =
      catalog.GetSnapshot()->GetTable(database, StaticStrings::kPublic, edge);

    if (!collection) {
      return {ERROR_BAD_PARAMETER, "Cannot find collection '", edge, "'"};
    }

    if (collection->GetTableType() != TableType::Edge) {
      return {ERROR_BAD_PARAMETER, "Collection '", edge,
              "' is not an edge collection."};
    }

    return {};
  };
}

Result ToInternal(vpack::Slice definition, ObjectId database_id, auto& meta) {
  Update<UserEntry> properties;

  if (auto r = vpack::ReadObjectNothrow(definition, properties); !r.ok()) {
    return r;
  }

  auto check = MakeCollectionChecker(database_id);

  meta.edges.reserve(properties.edges->size());

  for (auto& edge : *properties.edges) {
    std::shared_ptr<catalog::Table> c;
    if (auto r = check(edge.collection, c); !r.ok()) {
      return r;
    }

    meta.edges.emplace_back(c->planId(), edge.direction);
  }

  absl::c_sort(meta.edges);

  if (auto it = absl::c_adjacent_find(meta.edges); it != meta.edges.end())
    [[unlikely]] {
    auto& catalog =
      SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
    auto c = catalog.GetSnapshot()->GetObject(it->cid);
    return {ERROR_BAD_PARAMETER,
            "Collection specified multiple times: ", c ? c->GetName() : ""};
  }

  return {};
}

}  // namespace

Result GraphView::Make(std::shared_ptr<catalog::View>& view,
                       ObjectId database_id, ViewOptions&& options,
                       bool is_user_request) {
  Internal meta;
  auto r = [&] -> Result {
    if (is_user_request) {
      return ToInternal(options.properties, database_id, meta);
    } else {
      return vpack::ReadTupleNothrow(options.properties.get("internal"), meta);
    }
  }();

  if (!r.ok()) {
    return r;
  }

  SDB_ASSERT(absl::c_is_sorted(meta.edges));
  SDB_ASSERT(absl::c_adjacent_find(meta.edges) == meta.edges.end());

  view = std::make_shared<GraphView>(database_id, std::move(options.meta),
                                     std::move(meta));
  return {};
}

GraphView::GraphView(ObjectId database_id, ViewMeta&& meta, Internal&& internal)
  : View{std::move(meta), database_id}, _meta{std::move(internal)} {}

bool GraphView::visitCollections(const CollectionVisitor& visitor) const {
  for (auto& edge : _meta.edges) {
    if (!visitor(edge.cid, nullptr)) {
      return false;
    }
  }

  return true;
}

void GraphView::WriteInternal(vpack::Builder& b) const {
  SDB_ASSERT(b.isOpenObject());
  vpack::WriteObject(b, vpack::Embedded{ViewMeta::Make(*this)});
  b.add("internal");
  vpack::WriteTuple(b, _meta);
}

void GraphView::WriteProperties(vpack::Builder& b) const {
  SDB_ASSERT(b.isOpenObject());

  sdb::catalog::Update<UserEntryImpl> properties;

  const auto r = [&] {
    auto& catalog =
      SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
    auto snapshot = catalog.GetSnapshot();
    vpack::WriteObject(b, vpack::Embedded{ViewMeta::Make(*this)});

    properties.edges->reserve(_meta.edges.size());

    for (auto& edge : _meta.edges) {
      auto collection = snapshot->GetObject<catalog::Table>(edge.cid);

      if (!collection) {
        // collection doesn't exist anymore
        continue;
      }

      properties.edges->emplace_back(edge.direction, collection->GetName());
    }
    return Result{};
  }();
  SDB_ENSURE(r.ok(), ERROR_INTERNAL);

  vpack::WriteObject(b, vpack::Embedded{&properties});
}

Result GraphView::Rename(std::shared_ptr<catalog::View>& new_view,
                         std::string_view new_name) const {
  SDB_ASSERT(new_name != GetName());
  new_view = std::make_shared<GraphView>(GetDatabaseId(),
                                         ViewMeta{
                                           .id = GetId().id(),
                                           .name = std::string{new_name},
                                           .type = _type,
                                         },
                                         Internal{_meta});
  return {};
}

Result GraphView::Update(std::shared_ptr<catalog::View>& new_view,
                         vpack::Slice properties) const {
  Internal new_meta;
  auto r = ToInternal(properties, GetDatabaseId(), new_meta);
  if (!r.ok()) {
    return r;
  }

  new_view = std::make_shared<GraphView>(
    GetDatabaseId(),
    ViewMeta{.id = GetId().id(), .name = _name, .type = GetViewType()},
    std::move(new_meta));
  return {};
}

}  // namespace sdb::catalog
