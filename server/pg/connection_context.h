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

#include <atomic>

#include "basics/message_buffer.h"
#include "catalog/identifiers/object_id.h"
#include "pg/copy_messages_queue.h"
#include "pg/sql_error.h"
#include "query/transaction.h"
#include "utils/exec_context.h"

namespace sdb::pg {

class CopyInBridge;
}

namespace sdb {

class ConnectionContext final : public ExecContext, public query::Transaction {
 public:
  ConnectionContext(duckdb::ClientContext& duckdb_ctx, std::string_view user,
                    std::string_view dbname, ObjectId database_id,
                    std::shared_ptr<catalog::Database> database,
                    message::Buffer* send_buffer,
                    pg::CopyMessagesQueue* copy_queue);

  ~ConnectionContext() final { SDB_ASSERT(!HasNotices()); }

  std::string GetCurrentSchema() const;
  std::string GetCurrentSchemaFromSnapshot(
    std::shared_ptr<const catalog::Snapshot> snapshot) const;

  const auto& GetDatabasePtr() const { return _database; }

  auto* GetSendBuffer() const { return _send_buffer; }

  auto* GetCopyQueue() const { return _copy_queue; }

  // COPY FROM STDIN bridge for the new coroutine server (the old server uses
  // _copy_queue instead). Set per COPY statement, cleared after.
  auto* GetCopyInBridge() const { return _copy_in_bridge; }
  void SetCopyInBridge(pg::CopyInBridge* bridge) { _copy_in_bridge = bridge; }

  // Per-statement side channel for table functions that must hand serialized
  // response payload to the protocol handler beyond the statement's result
  // (es_bulk appends ES bulk-items JSON here while emitting rows; INSERT has
  // no RETURNING on serenedb tables). Set before the statement, cleared after.
  auto* GetResponseSink() const { return _response_sink; }
  void SetResponseSink(std::string* sink) { _response_sink = sink; }

  // Notices are an intrusive MPSC stack (Strand-style): producers on any
  // thread CAS-push; the single consumer exchanges the head out and reverses
  // for FIFO. The common SELECT/DML path pays one relaxed-ish load to learn
  // the stack is empty -- no mutex, no allocation.
  void AddNotice(pg::SqlErrorData notice) {
    auto* node = new NoticeNode{std::move(notice), nullptr};
    node->next = _notices.load(std::memory_order_relaxed);
    while (!_notices.compare_exchange_weak(
      node->next, node, std::memory_order_release, std::memory_order_relaxed)) {
    }
  }

  bool HasNotices() const {
    return _notices.load(std::memory_order_relaxed) != nullptr;
  }

  // Single consumer: detaches the stack, reverses the links in place (FIFO),
  // and feeds each notice to `fn` -- no intermediate storage.
  template<typename Fn>
  void ConsumeNotices(Fn&& fn) {
    auto* node = _notices.exchange(nullptr, std::memory_order_acquire);
    NoticeNode* fifo = nullptr;
    while (node != nullptr) {
      auto* next = std::exchange(node->next, fifo);
      fifo = std::exchange(node, next);
    }
    while (fifo != nullptr) {
      fn(fifo->data);
      delete std::exchange(fifo, fifo->next);
    }
  }

 private:
  struct NoticeNode {
    pg::SqlErrorData data;
    NoticeNode* next;
  };

  std::shared_ptr<catalog::Database> _database;
  message::Buffer* const _send_buffer;
  pg::CopyMessagesQueue* const _copy_queue;
  pg::CopyInBridge* _copy_in_bridge = nullptr;
  std::string* _response_sink = nullptr;
  std::atomic<NoticeNode*> _notices{nullptr};
};

}  // namespace sdb
