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

#include <duckdb/parser/parsed_data/create_info.hpp>
#include <memory>
#include <string>
#include <string_view>

#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

// One database, in the form a catalog entry is built from. duckdb's own
// DATABASE_ENTRY names an attachment rather than a SereneDB database, so this
// is a CreateInfo of ours under the same CatalogType: what a mutator fills in,
// what the catalog log records, and what SereneDBDatabaseEntry holds.
//
// Owner and ACL are not here: they travel beside the info and live on the
// entry.
class CreateDatabaseInfo final : public duckdb::CreateInfo {
 public:
  CreateDatabaseInfo()
    : duckdb::CreateInfo{duckdb::CatalogType::DATABASE_ENTRY} {}
  CreateDatabaseInfo(ObjectId id, std::string_view name);

  void Serialize(duckdb::Serializer& sink) const final;
  std::string ToString() const final;
  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;
  std::shared_ptr<CreateDatabaseInfo> CloneDatabase() const;

  static duckdb::unique_ptr<duckdb::CreateInfo> Deserialize(
    duckdb::Deserializer& src);

  ObjectId GetId() const noexcept { return ObjectId{oid}; }
  void SetId(ObjectId id) noexcept { oid = id.id(); }

  std::string_view GetName() const noexcept {
    return GetQualifiedName().Name().GetIdentifierName();
  }
  void SetDatabaseName(std::string_view name) {
    SetName(duckdb::Identifier{std::string{name}});
  }
};

}  // namespace sdb::catalog
