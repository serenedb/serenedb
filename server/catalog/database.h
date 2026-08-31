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

class CreateDatabaseInfo final : public duckdb::CreateInfo {
 public:
  CreateDatabaseInfo()
    : duckdb::CreateInfo{duckdb::CatalogType::DATABASE_ENTRY} {}
  CreateDatabaseInfo(ObjectId id, std::string_view name,
                     ObjectId public_schema_id);

  void Serialize(duckdb::Serializer& sink) const final;
  std::string ToString() const final;
  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;

  static duckdb::unique_ptr<duckdb::CreateInfo> Deserialize(
    duckdb::Deserializer& src);

  ObjectId GetId() const noexcept { return ObjectId{oid}; }
  void SetId(ObjectId id) noexcept { oid = id.id(); }

  std::string_view GetName() const noexcept {
    return GetQualifiedName().Name().GetIdentifierName();
  }

  // The id of the schema every database has from the moment it exists. The
  // schema itself is not a record: it is made when the catalog is opened, the
  // way duckdb makes its own default schema -- but its id is what pg_namespace
  // reports, so the database states it and every boot agrees.
  ObjectId PublicSchemaId() const noexcept { return _public_schema_id; }
  void SetPublicSchemaId(ObjectId id) noexcept { _public_schema_id = id; }

 private:
  ObjectId _public_schema_id;
};

}  // namespace sdb::catalog
