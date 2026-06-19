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

#include <absl/synchronization/mutex.h>

#include "basics/message_buffer.h"
#include "catalog/identifiers/object_id.h"
#include "pg/copy_messages_queue.h"
#include "pg/sql_error.h"
#include "query/transaction.h"
#include "utils/exec_context.h"

namespace sdb {

class ConnectionContext final : public ExecContext, public query::Transaction {
 public:
  ConnectionContext(duckdb::ClientContext& duckdb_ctx, std::string_view user,
                    ObjectId role_id, std::string_view dbname,
                    ObjectId database_id,
                    std::shared_ptr<catalog::Database> database,
                    message::Buffer* send_buffer,
                    pg::CopyMessagesQueue* copy_queue);

  ~ConnectionContext() final { SDB_ASSERT(_notices.empty()); }

  std::string GetCurrentSchema() const;
  std::string GetCurrentSchemaFromSnapshot(
    std::shared_ptr<const catalog::Snapshot> snapshot) const;

  ObjectId GetRoleId() const { return _role_id; }

  const auto& GetDatabasePtr() const { return _database; }

  auto* GetSendBuffer() const { return _send_buffer; }

  auto* GetCopyQueue() const { return _copy_queue; }

  void AddNotice(pg::SqlErrorData notice) {
    std::lock_guard lock{_mutex};
    _notices.push_back(std::move(notice));
  }

  auto StealNotices() {
    std::lock_guard lock{_mutex};
    return std::exchange(_notices, {});
  }

 private:
  std::shared_ptr<catalog::Database> _database;
  message::Buffer* const _send_buffer;
  pg::CopyMessagesQueue* const _copy_queue;
  absl::Mutex _mutex;
  std::vector<pg::SqlErrorData> _notices;
  const ObjectId _role_id;
};

}  // namespace sdb
