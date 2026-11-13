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

#include <absl/cleanup/cleanup.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "catalog/entry/role.h"
#include "catalog/transaction.h"
#include "pg/progress_registry.h"
#include "query/config.h"
#include "replication/origin_session.h"
#include "server/utils/message_buffer.h"
#include "server/utils/pointer_union.h"
#include "utils/drain_gate.h"

namespace sdb::otel {

struct LogRecord;
struct Span;
struct Metric;
template<typename Record>
struct ExportRequest;

}  // namespace sdb::otel
namespace sdb::connector {

struct EsBulkInput;

}  // namespace sdb::connector
namespace sdb::replication {

struct ReplBatch;

}  // namespace sdb::replication
namespace sdb::network::pg {

class ProtocolState;
class WireSinkContext;

}  // namespace sdb::network::pg
namespace sdb::pg {

class CopyInBatches;

struct LoginCheck {
  duckdb::idx_t database = 0;
  duckdb::idx_t role = 0;
  bool superuser = false;
  irs::pg::SqlErrorData error;
};

LoginCheck RequireLogin(std::string_view user, std::string_view dbname);

}  // namespace sdb::pg
namespace sdb::network {

class CancelRegistry;
}

namespace sdb {

using SideChannel =
  PointerUnion<pg::CopyInBatches, otel::ExportRequest<otel::LogRecord>,
               otel::ExportRequest<otel::Span>,
               otel::ExportRequest<otel::Metric>, connector::EsBulkInput,
               replication::ReplBatch>;

struct SessionIdentity {
  std::string_view user;
  std::string_view system_user;
  duckdb::idx_t role = 0;
  std::string_view database;
  duckdb::idx_t database_id = 0;
  message::Buffer* send_buffer = nullptr;
  int32_t backend_pid = 0;
  network::CancelRegistry* cancel_registry = nullptr;
  bool client_backend = false;
};

class ConnectionContext final : public duckdb::ClientContext {
 public:
  ConnectionContext(duckdb::DatabaseInstance& db,
                    const SessionIdentity& identity);
  ~ConnectionContext() final;

  static ConnectionContext& Get(duckdb::ClientContext& context) noexcept {
    return static_cast<ConnectionContext&>(context);
  }

  const std::string& user() const { return _user; }
  const std::string& SystemUser() const { return _system_user; }
  const std::string& GetDatabase() const { return _database_name; }
  duckdb::idx_t GetDatabaseId() const { return _database_id; }
  int32_t GetBackendPid() const { return _backend_pid; }

  auto* GetCancelRegistry() const { return _cancel_registry; }

  duckdb::idx_t GetRoleId() const { return _effective_role_id; }
  duckdb::idx_t GetLoginRoleId() const { return _login_role_id; }
  duckdb::idx_t GetSessionRoleId() const { return _session_role_id; }

  std::string EffectiveUserName();
  std::string SessionUserName();

  void SetEffectiveRole(duckdb::idx_t role) { _effective_role_id = role; }
  void SetSessionRole(duckdb::idx_t role) {
    _session_role_id = role;
    _effective_role_id = role;
  }
  void ResetIdentity() {
    _session_role_id = _login_role_id;
    _effective_role_id = _login_role_id;
  }

  auto* GetSendBuffer() const { return _send_buffer; }

  template<typename T>
  void SetSideChannel(T* value) {
    _side_channel.Set(value);
  }

  template<typename T>
  T* GetSideChannel() const {
    return _side_channel.Get<T>();
  }

  bool InTransactionBlock() const noexcept { return _in_transaction_block; }
  void SetInTransactionBlock(bool value) noexcept {
    _in_transaction_block = value;
  }

  void AddNotice(irs::pg::SqlErrorData notice) {
    auto* node = new NoticeNode{std::move(notice), nullptr};
    node->next = _notices.load(std::memory_order_relaxed);
    while (!_notices.compare_exchange_weak(
      node->next, node, std::memory_order_release, std::memory_order_relaxed)) {
    }
  }

  bool HasNotices() const { return _notices.load(std::memory_order_relaxed); }

  void DiscardNotices() {
    ConsumeNotices([](const irs::pg::SqlErrorData&) {});
  }

  template<typename Fn>
  void ConsumeNotices(Fn&& fn) {
    auto* node = _notices.exchange(nullptr, std::memory_order_acquire);
    NoticeNode* fifo = nullptr;
    while (node) {
      auto* next = std::exchange(node->next, fifo);
      fifo = std::exchange(node, next);
    }
    absl::Cleanup free_rest = [&fifo] {
      while (fifo) {
        delete std::exchange(fifo, fifo->next);
      }
    };
    while (fifo) {
      std::unique_ptr<NoticeNode> current{std::exchange(fifo, fifo->next)};
      fn(current->data);
    }
  }

  int8_t GetExtraFloatDigits() const;
  ByteaOutput GetByteaOutput();
  std::string GetTimeZone() const;
  IsolationLevel GetIsolationLevel() const {
    return transaction.GetIsolationLevel();
  }
  bool IsExplicitTransaction() const { return !transaction.IsAutoCommit(); }
  bool IsStableSnapshot() const;

  uint64_t CatalogEpoch() const noexcept { return _catalog_epoch; }
  void RefreshCatalogEpoch() noexcept;

  std::optional<std::string> Get(std::string_view key);

  void SetSetting(std::string_view key, std::string value, bool);
  void SetSettingChecked(std::string_view key, std::string value,
                         bool is_local);

  uint64_t SettingsVersion() const noexcept { return _settings_version; }
  void MarkSettingsChanged() noexcept { ++_settings_version; }

  std::shared_ptr<catalog::SessionTransaction> AcquireTransaction();
  catalog::SessionTransaction* CurrentTransaction() const noexcept {
    return _transaction.get();
  }
  catalog::SessionTransaction& Transaction() const noexcept {
    SDB_ASSERT(_transaction);
    return *_transaction;
  }

  bool HadQueryInTransaction() const noexcept {
    return _had_query_in_transaction;
  }
  void MarkQueryInTransaction() noexcept { _had_query_in_transaction = true; }
  [[nodiscard]] auto InternalStatements() noexcept {
    ++_internal_statements;
    return absl::Cleanup{[this] { --_internal_statements; }};
  }
  pg::ProgressMetrics& Progress() const noexcept { return _progress->metrics; }
  const std::shared_ptr<pg::ProgressSource>& ProgressSource() const noexcept {
    return _progress;
  }

  int copy_stdin_open_count = 0;
  bool copy_stdin_done = false;

  std::shared_ptr<network::pg::WireSinkContext> wire_sink;

  const network::pg::ProtocolState* protocol_state = nullptr;

  pg::ProgressCommand pending_copy_command = pg::ProgressCommand::None;
  pg::ProgressIoType pending_copy_io = pg::ProgressIoType::None;
  duckdb::idx_t pending_copy_relid = 0;

  uint32_t job_depth = 0;

  bool lists_system_schemas = true;

  std::unique_ptr<replication::OriginSession> replication_origin;

  void ValidateIsolationLevel(duckdb::TransactionIsolationLevel level) final;
  bool EmitWarning(const char* message) final;
  bool IsSettingVisible(const std::string& name) final;
  void OnSettingChange(const std::string& name, duckdb::SetScope scope) final;
  void OnQueryBegin() final;
  void OnQueryEnd() final;
  void OnTransactionPreCommit(duckdb::MetaTransaction& meta) final;
  void OnTransactionPreRollback(duckdb::MetaTransaction& meta) final;
  void OnTransactionCommit(duckdb::MetaTransaction& meta) final;
  void OnTransactionRollback(duckdb::MetaTransaction& meta) final;
  void OnBoundPlan(duckdb::Binder& binder, duckdb::LogicalOperator& plan) final;

 private:
  struct NoticeNode {
    irs::pg::SqlErrorData data;
    NoticeNode* next;
  };

  struct TxnVariable {
    duckdb::Value rollback_restore;
    std::optional<duckdb::Value> commit_restore;
  };

  void EndTransaction() noexcept;
  void ReleaseSnapshot() noexcept;

  void OnSet(std::string_view name, bool is_local, duckdb::Value old_value);
  void RestoreValue(std::string_view key, duckdb::Value value) noexcept;
  void RollbackVariables() noexcept;
  void CommitVariables() noexcept;

  const std::string _user;
  const std::string _system_user;
  const std::string _database_name;
  const duckdb::idx_t _database_id;
  const int32_t _backend_pid;
  network::CancelRegistry* const _cancel_registry;
  message::Buffer* const _send_buffer;
  const duckdb::idx_t _login_role_id;
  duckdb::idx_t _session_role_id;
  duckdb::idx_t _effective_role_id;
  SideChannel _side_channel;
  bool _in_transaction_block = false;
  std::atomic<NoticeNode*> _notices{nullptr};
  std::shared_ptr<pg::ProgressSource> _progress;
  uint64_t _catalog_epoch = 0;
  uint64_t _settings_version = 0;
  irs::containers::NodeHashMap<std::string, TxnVariable> _variables;
  catalog::SessionSlot& _slot;
  absl::Mutex _transaction_mutex;
  std::shared_ptr<catalog::SessionTransaction> _transaction;
  duckdb::transaction_t _transaction_owner = duckdb::MAX_TRANSACTION_ID;
  utils::DrainGate::Pass _commit_pass;
  uint32_t _internal_statements = 0;
  bool _query_in_transaction = false;
  bool _had_query_in_transaction = false;
};

duckdb::shared_ptr<duckdb::ClientContext> CreateClientContext(
  duckdb::DatabaseInstance& db);

duckdb::unique_ptr<duckdb::Connection> MakeConnection(
  duckdb::DatabaseInstance& db, const SessionIdentity& identity);

void StartSession(ConnectionContext& ctx, bool superuser);

duckdb::unique_ptr<duckdb::Connection> OpenClientSession(
  std::string_view user, std::string_view database, int32_t backend_pid,
  network::CancelRegistry* cancel_registry,
  duckdb::shared_ptr<duckdb::SchedulerAccount> account);

struct NoticeTarget {
  std::string_view what;
  std::string_view name;
  ConnectionContext* forward_to = nullptr;
};

void HandNotices(ConnectionContext& ctx, const NoticeTarget& target);

struct SystemSession {
  std::string_view user;
  std::string_view system_user;
  duckdb::idx_t role = 0;
  std::string_view database;
  duckdb::idx_t database_id = 0;
  int32_t backend_pid = 0;
};

struct SystemConnection {
  explicit SystemConnection(const SystemSession& session,
                            const NoticeTarget& notices);
  SystemConnection(const SystemConnection&) = delete;
  SystemConnection& operator=(const SystemConnection&) = delete;
  ~SystemConnection();

  duckdb::unique_ptr<duckdb::Connection> conn;
  pg::ProgressRegistration progress;
  NoticeTarget notices;
};

}  // namespace sdb
