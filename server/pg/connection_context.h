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

#include "basics/message_buffer.h"
#include "catalog/identifiers/object_id.h"
#include "pg/copy_messages_queue.h"
#include "pg/sql_error.h"
#include "query/transaction.h"
#include "utils/exec_context.h"

namespace sdb {

class ConnectionContext : public ExecContext, public query::Transaction {
 public:
  ConnectionContext(std::string_view user, std::string_view dbname,
                    ObjectId database_id, message::Buffer* send_buffer,
                    pg::CopyMessagesQueue* copy_queue);

  std::string GetCurrentSchema() const;

  message::Buffer* GetSendBuffer() { return _send_buffer; }

  pg::CopyMessagesQueue* GetCopyQueue() { return _copy_queue; }

  void AddNotice(pg::SqlErrorData notice) {
    _notices.push_back(std::move(notice));
  }

  auto StealNotices() { return std::exchange(_notices, {}); }

 private:
  std::vector<pg::SqlErrorData> _notices;
  message::Buffer* _send_buffer;
  pg::CopyMessagesQueue* _copy_queue;
};

}  // namespace sdb
