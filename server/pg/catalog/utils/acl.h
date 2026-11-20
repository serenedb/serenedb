////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <absl/functional/function_ref.h>

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <optional>
#include <string>
#include <string_view>

#include "pg/catalog/oids.h"

namespace sdb::pg {

void AppendAcl(std::string& out, const duckdb::AclItem& item,
               const duckdb::SecuritySnapshot& roles);

std::optional<irs::pg::SqlErrorData> ParseAcl(
  std::string_view text, const duckdb::SecuritySnapshot& roles, bool oids,
  duckdb::AclItem& item);

void VisitDefaultAcls(
  duckdb::ClientContext& context, const duckdb::CatalogEntry& database,
  absl::FunctionRef<void(duckdb::idx_t oid, const duckdb::DefaultAcl& row)>
    visitor);

struct RoleReference {
  duckdb::idx_t dbid;
  duckdb::idx_t classid;
  duckdb::idx_t objid;
  int32_t objsubid;
  char deptype;
};

void VisitRoleReferences(
  duckdb::ClientContext& context, duckdb::idx_t role,
  absl::FunctionRef<void(const RoleReference& reference,
                         absl::FunctionRef<std::string()> describe)>
    visitor);

}  // namespace sdb::pg
