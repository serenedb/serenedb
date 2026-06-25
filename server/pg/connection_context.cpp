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

#include "pg/connection_context.h"

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "query/transaction.h"

namespace sdb {

ConnectionContext::ConnectionContext(
  duckdb::ClientContext& duckdb_ctx, std::string_view user, ObjectId role_id,
  std::string_view dbname, ObjectId database_id,
  std::shared_ptr<catalog::Database> database, message::Buffer* send_buffer,
  pg::CopyMessagesQueue* copy_queue, int32_t backend_pid,
  network::pg::CancelRegistry* cancel_registry)
  : Transaction{duckdb_ctx},
    _user{user},
    _database_name{dbname},
    _database_id{database_id},
    _backend_pid{backend_pid},
    _cancel_registry{cancel_registry},
    _database{std::move(database)},
    _send_buffer{send_buffer},
    _copy_queue{copy_queue},
    _role_id{role_id} {}

std::string ConnectionContext::GetCurrentSchemaFromSnapshot(
  std::shared_ptr<const catalog::Snapshot> snapshot) const {
  SDB_ASSERT(snapshot);
  auto database_id = GetDatabaseId();
  auto search_path = GetSearchPath();
  auto it = absl::c_find_if(search_path, [&](const std::string& schema_name) {
    return snapshot->GetSchema(database_id, schema_name);
  });

  return it != search_path.end() ? *it : "";
}

std::string ConnectionContext::GetCurrentSchema() const {
  auto snapshot = EnsureCatalogSnapshot();
  return GetCurrentSchemaFromSnapshot(snapshot);
}

}  // namespace sdb
