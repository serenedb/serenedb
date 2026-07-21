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

#include <duckdb/common/unique_ptr.hpp>
#include <duckdb/common/vector.hpp>

namespace duckdb {
class DatabaseInstance;
class ClientContext;
class Binder;
class LogicalGet;
class LogicalOperator;
class TableCatalogEntry;
class BoundConstraint;
}  // namespace duckdb

namespace sdb::connector {

// Installs the DBConfig::rls_wrap_scan hook that wraps a base-table scan in the
// table's Row-Level Security filter during binding.
void RegisterRlsEnforcement(duckdb::DatabaseInstance& db);

// The hook itself (exposed for testing). Returns `plan` unchanged when RLS does
// not apply, or `plan` wrapped in a LogicalFilter carrying the RLS predicate.
duckdb::unique_ptr<duckdb::LogicalOperator> RlsWrapScan(
  duckdb::ClientContext& context, duckdb::Binder& binder, duckdb::LogicalGet& get,
  duckdb::TableCatalogEntry& table, duckdb::unique_ptr<duckdb::LogicalOperator> plan);

// Appends BoundCheckConstraints validating the INSERT/UPDATE post-image against
// the table's WITH CHECK policies.
void RlsAppendCheckConstraints(
  duckdb::ClientContext& context, duckdb::Binder& binder,
  duckdb::TableCatalogEntry& table, bool is_update,
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>& bound_constraints);

}  // namespace sdb::connector
