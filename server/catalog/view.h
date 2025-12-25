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

#pragma once

#include "basics/containers/small_vector.h"
#include "basics/result.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/index_id.h"
#include "catalog/object.h"
#include "catalog/types.h"
#include "vpack/serializer.h"
#include "vpack/slice.h"

namespace sdb::catalog {

enum class ViewContext {
  Internal,
  User,
  Restore,
};

enum class ViewType : uint8_t {
  ViewSearch = 0,
  ViewGraph,
  ViewSqlQuery,
};

// NOLINTBEGIN
struct ViewMeta {
  vpack::Optional<Identifier> id;
  std::string name;
  ViewType type = ViewType::ViewSearch;

  static ViewMeta Make(const View& view);
};

struct ViewOptions {
  ViewMeta meta;
  vpack::Slice properties = vpack::Slice::emptyObjectSlice();

  static Result Read(ViewOptions& options, vpack::Slice slice);
};
// NOLINTEND

class View : public SchemaObject {
 public:
  using Indexes = containers::SmallVector<IndexId, 1>;
  // TODO(mbkkt) absl::FunctionRef
  // visitor for map<CollectionId, set<IndexId>>, Indexes is movable
  using CollectionVisitor = std::function<bool(ObjectId, Indexes*)>;

  auto GetViewType() const noexcept { return _type; }

  virtual bool visitCollections(const CollectionVisitor& visitor) const = 0;

  virtual Result Rename(std::shared_ptr<catalog::View>& new_view,
                        std::string_view new_name) const = 0;

  virtual Result Update(std::shared_ptr<catalog::View>& new_view,
                        vpack::Slice properties) const = 0;

 protected:
  View(ViewMeta&& options, ObjectId database_id);

  ViewType _type;
};

Result CreateViewInstance(std::shared_ptr<catalog::View>& view,
                          ObjectId database_id, ViewOptions&& options,
                          ViewContext ctx);

}  // namespace sdb::catalog

namespace magic_enum {

template<>
constexpr customize::customize_t customize::enum_name<sdb::catalog::ViewType>(
  sdb::catalog::ViewType value) noexcept {
  switch (value) {
    case sdb::catalog::ViewType::ViewGraph:
      return "graph";
    case sdb::catalog::ViewType::ViewSearch:
      return "search-alias";
    case sdb::catalog::ViewType::ViewSqlQuery:
      return "sql-query";
    default:
      return invalid_tag;
  }
}

}  // namespace magic_enum
