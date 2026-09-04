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

#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/logical_operator.hpp>

namespace sdb::connector {

// CREATE INDEX over a view. duckdb hands us the bound view body, which answers
// a query but cannot be re-scanned: a build has to read a source it can list
// and pin, so REINDEX can revisit exactly what it indexed. That source is what
// ResolveViewFastPath picks, and the body plan is discarded for it.
duckdb::unique_ptr<duckdb::LogicalOperator> BindCreateIndexOnView(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::ViewCatalogEntry& view);

}  // namespace sdb::connector
