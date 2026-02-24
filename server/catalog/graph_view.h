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

#pragma once

#include <vpack/builder.h>

#include <vector>

#include "catalog/view.h"

namespace sdb::catalog {

class GraphView final : public View {
 private:
  struct Edge {
    bool operator==(const Edge& rhs) const noexcept { return cid == rhs.cid; }
    auto operator<=>(const Edge& rhs) const noexcept { return cid <=> rhs.cid; }

    ObjectId cid;
    std::optional<EdgeDirection> direction;
  };
  struct Internal {
    std::vector<Edge> edges;
  };

 public:
  static constexpr auto type() noexcept { return ViewType::ViewGraph; }

  static Result Make(std::shared_ptr<catalog::View>& view, ObjectId database_id,
                     ViewOptions&& options, bool is_user_request);

  GraphView(ObjectId database_id, ViewMeta&& meta, Internal&& internal);

  auto GetEdges() const noexcept { return std::span{_meta.edges}; }

  void WriteProperties(vpack::Builder&) const final;

  void WriteInternal(vpack::Builder&) const final;

  bool visitCollections(const CollectionVisitor& visitor) const final;

  Result Rename(std::shared_ptr<catalog::View>& new_view,
                std::string_view new_name) const final;

  Result Update(std::shared_ptr<catalog::View>& new_view,
                vpack::Slice properties) const final;

 private:
  Internal _meta;
};

}  // namespace sdb::catalog
