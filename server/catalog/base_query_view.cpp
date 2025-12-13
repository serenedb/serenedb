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

#include "base_query_view.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <vpack/slice.h>

#include <algorithm>
#include <cstdint>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <mutex>
#include <string_view>
#include <type_traits>
#include <vector>

#include "basics/result.h"
#include "basics/std.hpp"
#include "catalog/catalog.h"
#include "catalog/identifiers/identifier.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/sql_query_view.h"
#include "catalog/view.h"
#include "vpack/serializer.h"

namespace sdb::catalog {

template<typename ImplQueryView>
Result BaseQueryView<ImplQueryView>::Make(std::shared_ptr<catalog::View>& view,
                                          ObjectId database_id,
                                          ViewOptions&& options,
                                          ViewContext ctx) {
  Internal meta;
  auto r = [&] -> Result {
    if (ctx != ViewContext::Internal) {
      return vpack::ReadObjectNothrow(options.properties, meta);
    } else {
      return vpack::ReadTupleNothrow(options.properties.get("internal"), meta);
    }
  }();

  if (!r.ok()) {
    return r;
  }

  if (meta.query.empty()) {
    return {ERROR_BAD_PARAMETER, "Query can't be empty"};
  }

  auto state = ImplQueryView::Create();

  r = ImplQueryView::Parse(*state, database_id, meta.query);
  if (!r.ok()) {
    return r;
  }

  if (ctx == ViewContext::User) {
    r = ImplQueryView::Check(database_id, options.meta.name, *state);
    if (!r.ok()) {
      return r;
    }
  }

  view = std::make_shared<BaseQueryView>(database_id, std::move(options.meta),
                                         std::move(meta), std::move(state));
  return {};
}

template<typename ImplQueryView>
BaseQueryView<ImplQueryView>::BaseQueryView(ObjectId database_id,
                                            ViewMeta&& options, Internal&& meta,
                                            StatePtr state)
  : View{std::move(options), database_id},
    _meta{std::move(meta)},
    _state{std::move(state)} {}

template<typename ImplQueryView>
void BaseQueryView<ImplQueryView>::WriteInternal(vpack::Builder& b) const {
  SDB_ASSERT(b.isOpenObject());
  vpack::WriteObject(b, vpack::Embedded{ViewMeta::Make(*this)});
  b.add("internal");
  vpack::WriteTuple(b, _meta);
}

template<typename ImplQueryView>
void BaseQueryView<ImplQueryView>::WriteProperties(vpack::Builder& b) const {
  SDB_ASSERT(b.isOpenObject());
  vpack::WriteObject(b, vpack::Embedded{ViewMeta::Make(*this)});
  vpack::WriteObject(b, vpack::Embedded{&_meta});
}

template<typename ImplQueryView>
Result BaseQueryView<ImplQueryView>::Rename(
  std::shared_ptr<catalog::View>& new_view, std::string_view new_name) const {
  SDB_ASSERT(new_name != GetName());

  auto r = ImplQueryView::Check(ObjectId{GetDatabaseId()}, new_name, *_state);

  if (!r.ok()) {
    return r;
  }

  new_view = std::make_shared<BaseQueryView>(GetDatabaseId(),
                                             ViewMeta{
                                               .id = GetId().id(),
                                               .name = std::string{new_name},
                                               .type = GetViewType(),
                                             },
                                             Internal{_meta}, _state);
  return {};
}

template<typename ImplQueryView>
Result BaseQueryView<ImplQueryView>::Update(
  std::shared_ptr<catalog::View>& new_view, vpack::Slice new_options) const {
  Internal new_meta;
  auto r = vpack::ReadObjectNothrow(new_options, new_meta);
  if (!r.ok()) {
    return r;
  }

  if (new_meta == _meta) {
    return {};
  }

  auto new_state = ImplQueryView::Create();

  r =
    ImplQueryView::Parse(*new_state, ObjectId{GetDatabaseId()}, new_meta.query);
  if (!r.ok()) {
    return r;
  }

  r = ImplQueryView::Check(ObjectId{GetDatabaseId()}, GetName(), *new_state);

  if (!r.ok()) {
    return r;
  }

  new_view =
    std::make_shared<BaseQueryView>(GetDatabaseId(),
                                    ViewMeta{
                                      .id = GetId().id(),
                                      .name = _name,
                                      .type = GetViewType(),
                                    },
                                    std::move(new_meta), std::move(new_state));
  return {};
}

template class BaseQueryView<SqlQueryViewImpl>;

}  // namespace sdb::catalog
