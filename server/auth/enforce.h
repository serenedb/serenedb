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

#include <duckdb/catalog/catalog_entry.hpp>
#include <string>

namespace duckdb {

class AttachedDatabase;
class Binder;
class ClientContext;
class LogicalOperator;

}  // namespace duckdb
namespace sdb {

class ConnectionContext;

}  // namespace sdb
namespace sdb::auth {

bool EnforcePlan(duckdb::ClientContext& context, ConnectionContext& connection,
                 duckdb::Binder& binder, duckdb::LogicalOperator& plan);

bool Unowned(const duckdb::CatalogEntry& entry);

bool CanSee(duckdb::ClientContext& context, duckdb::CatalogEntry& entry);

bool CanSeeDatabase(duckdb::ClientContext& context,
                    duckdb::AttachedDatabase& database);

void CheckEntryReference(duckdb::ClientContext& context,
                         duckdb::CatalogEntry& entry);

void CheckFileAccess(duckdb::ClientContext& context, const std::string& path);

}  // namespace sdb::auth
