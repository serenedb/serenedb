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

#include "storage_engine.h"

#include <vpack/builder.h>
#include <vpack/slice.h>

#include <utility>

#include "app/app_server.h"
#include "app/name_validator.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "general_server/state.h"
#include "storage_engine/engine_selector_feature.h"
#include "vpack/vpack_helper.h"

#ifdef SDB_CLUSTER
#include "indexes/index_helper.h"
#endif

namespace sdb {
namespace {

struct InvalidIndexFactory final : public IndexFactory {
  bool equal(vpack::Slice, vpack::Slice, ObjectId) const final { return false; }

  std::shared_ptr<Index> instantiate(catalog::Table&, vpack::Slice definition,
                                     IndexId, bool) const final {
    auto type =
      basics::VPackHelper::getString(definition, StaticStrings::kIndexType, {});
    SDB_THROW(ERROR_BAD_PARAMETER, "invalid index type: ", type);
  }

  Result normalize(catalog::Table& collection, vpack::Builder&,
                   vpack::Slice definition) const final {
    auto type =
      basics::VPackHelper::getString(definition, StaticStrings::kIndexType, {});
    return {ERROR_BAD_PARAMETER, "invalid index type: ", type};
  }
};

}  // namespace

StorageEngine::StorageEngine(Server& server, std::string_view engine_name,
                             std::string_view feature_name)
  : SerenedFeature{server, feature_name}, _type_name(engine_name) {
  // each specific storage engine feature is optional. the storage engine
  // selection feature will make sure that exactly one engine is selected at
  // startup
  setOptional(true);
  // storage engines must not use elevated privileges for files etc
}

const IndexFactory& StorageEngine::index_factory(
  std::string_view type) const noexcept {
  auto it = _factories.find(type);

  SDB_ASSERT(it == _factories.end() || false == !(it->second));

  if (it == _factories.end()) {
    static const InvalidIndexFactory kInvalid;
    return kInvalid;
  }

  return *it->second;
}

std::shared_ptr<Index> StorageEngine::prepareIndexFromSlice(
  vpack::Slice definition, bool generate_key, catalog::Table& collection,
  bool is_cluster_constructor) const {
#ifdef SDB_CLUSTER
  auto id = index_helper::ValidateSlice(definition, generate_key,
                                        is_cluster_constructor);
  auto type = definition.get(StaticStrings::kIndexType);

  if (!type.isString()) {
    SDB_THROW(ERROR_BAD_PARAMETER, "invalid index type definition");
  }

  auto& factory = index_factory(type.stringView());
  std::shared_ptr<Index> index =
    factory.instantiate(collection, definition, id, is_cluster_constructor);

  if (!index) {
    SDB_THROW(ERROR_INTERNAL,
              "failed to instantiate index, factory returned null instance");
  }

  return index;
#else
  return {};
#endif
}

Result StorageEngine::enhanceIndexDefinition(catalog::Table& collection,
                                             vpack::Slice definition,
                                             vpack::Builder& normalized) const {
#ifdef SDB_CLUSTER
  auto type =
    basics::VPackHelper::getString(definition, StaticStrings::kIndexType, {});

  if (type.empty()) {
    return {ERROR_BAD_PARAMETER, "invalid index type"};
  }

  SDB_ASSERT(normalized.isEmpty());

  return basics::SafeCall([&] {
    vpack::ObjectBuilder b(&normalized);

    if (const auto id = index_helper::ExtractId(definition); id.isSet()) {
      absl::AlphaNum to_str(id.id());
      normalized.add(StaticStrings::kIndexId, to_str.Piece());
    }

    std::string name{basics::VPackHelper::getString(
      definition, StaticStrings::kIndexName, {})};

    if (name.empty()) {
      // we should set the name for special types explicitly elsewhere,
      // but just in case...
      if (auto t = Index::type(type); t == kTypePrimaryIndex) {
        name = StaticStrings::kIndexNamePrimary;
      } else if (t == kTypeEdgeIndex) {
        name = StaticStrings::kIndexNameEdge;
      } else {
        // generate a name
        name = absl::StrCat("idx_", NewTickHybridLogicalClock());
      }
    }

    if (!name.empty()) {
      if (auto res = IndexNameValidator::validateName(name); res.fail()) {
        return res;
      }
    }

    normalized.add(StaticStrings::kIndexName, name);

    return index_factory(type).normalize(collection, normalized, definition);
  });
#else
  return {ERROR_NOT_IMPLEMENTED};
#endif
}

std::shared_ptr<TableShard> GetTableShard(ObjectId id) {
  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = ServerState::instance()->IsCoordinator() ? catalogs.Global()
                                                           : catalogs.Local();
  return catalog.GetSnapshot()->GetTableShard(id);
}

}  // namespace sdb
