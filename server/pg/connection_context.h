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

namespace sdb::pg {

class CopyInBridge;
}

namespace sdb::network::pg {

class CancelRegistry;
}

namespace sdb {

class ConnectionContext final : public query::Transaction {
 public:
  ConnectionContext(duckdb::ClientContext& duckdb_ctx, std::string_view user,
                    ObjectId role_id, std::string_view dbname,
                    ObjectId database_id,
                    std::shared_ptr<catalog::Database> database,
                    message::Buffer* send_buffer,
                    pg::CopyMessagesQueue* copy_queue, int32_t backend_pid,
                    network::pg::CancelRegistry* cancel_registry);

  ~ConnectionContext() final { SDB_ASSERT(!HasNotices()); }

  const std::string& user() const { return _user; }
  const std::string& GetDatabase() const { return _database_name; }
  ObjectId GetDatabaseId() const { return _database_id; }
  int32_t GetBackendPid() const { return _backend_pid; }

  auto* GetCancelRegistry() const { return _cancel_registry; }

  std::string GetCurrentSchema() const;
  std::string GetCurrentSchemaFromSnapshot(
    std::shared_ptr<const catalog::Snapshot> snapshot) const;

  ObjectId GetRoleId() const { return _effective_role_id; }
  ObjectId GetLoginRoleId() const { return _login_role_id; }
  ObjectId GetSessionRoleId() const { return _session_role_id; }

  std::string EffectiveUserName() const;
  std::string SessionUserName() const;

  // SET ROLE r: switch the effective role. is_none marks "no explicit SET ROLE"
  // (effective == session role) so SHOW role renders 'none'.
  void SetEffectiveRole(ObjectId role, bool is_none) {
    _effective_role_id = role;
    _role_is_none = is_none;
  }
  void SetSessionRole(ObjectId role) {
    _session_role_id = role;
    _effective_role_id = role;
    _role_is_none = true;
  }
  void ResetIdentity() {
    _session_role_id = _login_role_id;
    _effective_role_id = _login_role_id;
    _role_is_none = true;
  }
  bool RoleIsNone() const { return _role_is_none; }

  const auto& GetDatabasePtr() const { return _database; }

  auto* GetSendBuffer() const { return _send_buffer; }

  auto* GetCopyQueue() const { return _copy_queue; }

  auto* GetCopyInBridge() const { return _copy_in_bridge; }
  void SetCopyInBridge(pg::CopyInBridge* bridge) { _copy_in_bridge = bridge; }

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

  const std::string _user;
  const std::string _database_name;
  const ObjectId _database_id;
  const int32_t _backend_pid;
  network::pg::CancelRegistry* const _cancel_registry;
  std::shared_ptr<catalog::Database> _database;
  message::Buffer* const _send_buffer;
  pg::CopyMessagesQueue* const _copy_queue;
  const ObjectId _login_role_id;
  ObjectId _session_role_id;
  ObjectId _effective_role_id;
  bool _role_is_none = true;
  pg::CopyInBridge* _copy_in_bridge = nullptr;
  std::string* _response_sink = nullptr;
  std::atomic<NoticeNode*> _notices{nullptr};
};

}  // namespace sdb
