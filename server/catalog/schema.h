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

#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <memory>
#include <string_view>
#include <utility>

#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/persistence/schema.h"

namespace sdb::basics {

class JsonSink;

}  // namespace sdb::basics
namespace sdb::catalog {

// One schema, in the form a catalog entry is built from. duckdb's own
// CreateSchemaInfo carries no stable id, so this adds it: what a mutator fills
// in, what the catalog log records, and what SereneDBSchemaEntry holds.
//
// Owner and ACL are not here: they travel beside the info, as side state on the
// entry, because a schema entry owns the CatalogSets of its whole contents and
// therefore is never replaced by a newer version.
class CreateSchemaInfo final : public duckdb::CreateSchemaInfo {
 public:
  CreateSchemaInfo(ObjectId id, ObjectId database_id, std::string_view name);

  persistence::SchemaOptions ToData() const;
  void Serialize(duckdb::Serializer& sink) const final;
  void WriteJson(basics::JsonSink& sink) const;
  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;
  std::shared_ptr<CreateSchemaInfo> CloneSchema() const;

  static std::shared_ptr<CreateSchemaInfo> Deserialize(
    duckdb::Deserializer& src, ObjectId id, ObjectId database_id);

  ObjectId GetId() const noexcept { return ObjectId{oid}; }
  void SetId(ObjectId id) noexcept { oid = id.id(); }

  ObjectId GetDatabaseId() const noexcept { return ObjectId{parent_oid}; }
  void SetDatabaseId(ObjectId id) noexcept { parent_oid = id.id(); }
  // A schema is a database child, so the database is its parent.
  ObjectId GetParentId() const noexcept { return GetDatabaseId(); }

  std::string_view GetName() const noexcept {
    return GetQualifiedName().Schema().GetIdentifierName();
  }
  void SetSchemaName(std::string_view name) {
    SetSchema(duckdb::Identifier{std::string{name}});
  }
};

// The info is published whole, copy-on-write, because a schema entry owns the
// CatalogSets of its contents and so is never replaced by a newer version -- an
// owner or ACL change becomes visible when it commits rather than when the
// reader's snapshot advances.
//
// The owner and the ACL are on the entry, their one home; a reader wanting
// both takes a HeldSchema.
using SchemaRef = std::shared_ptr<const CreateSchemaInfo>;
using HeldSchema = std::pair<SchemaRef, Permissions>;

}  // namespace sdb::catalog
