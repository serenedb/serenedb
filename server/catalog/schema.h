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

namespace sdb::catalog {

// A schema is duckdb's own CreateSchemaInfo: the name is its qualified name and
// the identities are the CreateInfo's, so there is nothing left for a subclass
// to hold.
//
// Owner and ACL are not on the info: they live on the entry, because a schema
// entry owns the CatalogSets of its whole contents and is therefore never
// replaced by a newer version.
std::shared_ptr<duckdb::CreateSchemaInfo> MakeSchemaInfo(ObjectId id,
                                                         ObjectId database_id,
                                                         std::string_view name);

inline std::string_view SchemaNameOf(
  const duckdb::CreateSchemaInfo& info) noexcept {
  return info.GetQualifiedName().Schema().GetIdentifierName();
}

// The info is published whole, copy-on-write, because a schema entry owns the
// CatalogSets of its contents and so is never replaced by a newer version -- an
// owner or ACL change becomes visible when it commits rather than when the
// reader's snapshot advances.
//
// The owner and the ACL are on the entry, their one home; a reader wanting
// both takes a HeldSchema.
using SchemaRef = std::shared_ptr<const duckdb::CreateSchemaInfo>;
using HeldSchema = std::pair<SchemaRef, Permissions>;

}  // namespace sdb::catalog
