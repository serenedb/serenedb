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
#include <string_view>

#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

// A schema is duckdb's own CreateSchemaInfo -- nothing left for a subclass to
// hold; owner and ACL live on the entry. A new version of a schema entry (a
// rename, an owner change) carries the CatalogSets of its contents over, so
// nothing under it is rebuilt.
duckdb::unique_ptr<duckdb::CreateSchemaInfo> MakeSchemaInfo(
  ObjectId id, ObjectId database_id, std::string_view name);

}  // namespace sdb::catalog
