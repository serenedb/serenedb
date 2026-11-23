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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>

#include "catalog/catalog.h"

namespace sdb::catalog {

duckdb::unique_ptr<duckdb::Catalog> AttachForeignDatabase(
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const duckdb::AttachInfo& info, duckdb::AttachOptions& options);

void ReplayForeignDatabase(duckdb::ClientContext& context,
                           SereneDBCatalog& catalog, const ForeignLog& foreign);

}  // namespace sdb::catalog
