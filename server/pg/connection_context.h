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
#include <duckdb/main/client_data.hpp>
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
class CopyOutput;

}  // namespace sdb::connector
namespace sdb::replication {

struct ReplBatch;

}  // namespace sdb::replication
namespace sdb::network::pg {

class WireSinkContext;

}  // namespace sdb::network::pg
namespace sdb::pg {

class CopyInBatches;

struct LoginCheck {
  duckdb::idx_t role;
  duckdb::idx_t database = 0;
  bool superuser = false;
  int32_t role_conn_limit = -1;
  int32_t database_conn_limit = -1;
  std::string role_name;
  irs::pg::SqlErrorData error;
};

LoginCheck RequireLoginRole(std::string_view user, std::string_view dbname);

std::optional<irs::pg::SqlErrorData> ConnectionLimitError(
  const LoginCheck& login, std::string_view dbname);

bool IsHbaMember(std::string_view user, std::string_view group);

}  // namespace sdb::pg
namespace sdb::network {

class CancelRegistry;
class SendGate;

}  // namespace sdb::network
namespace sdb {

using SideChannel =
  PointerUnion<pg::CopyInBatches, otel::ExportRequest<otel::LogRecord>,
               otel::ExportRequest<otel::Span>,
               otel::ExportRequest<otel::Metric>, connector::EsBulkInput,
               replication::ReplBatch, connector::CopyOutput>;

struct SessionIdentity {
  std::string_view user;
  std::string_view system_user;
  duckdb::idx_t role = 0;
  std::string_view database;
  duckdb::idx_t database_id = 0;
  message::Buffer* send_buffer = nullptr;
  network::SendGate* send_gate = nullptr;
  int32_t backend_pid = 0;
  network::CancelRegistry* cancel_registry = nullptr;
  bool client_backend = false;
  duckdb::shared_ptr<duckdb::AttachedDatabase> temporary;
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
  const std::string& GetDatabase() const {
    return client_data->default_database.GetIdentifierName();
  }
  duckdb::idx_t GetDatabaseId() const { return _database_id; }
  int32_t GetBackendPid() const { return _backend_pid; }

  auto* GetCancelRegistry() const { return _cancel_registry; }

  duckdb::idx_t GetRoleId() const { return current_role_id; }
  duckdb::idx_t GetLoginRoleId() const { return _login_role_id; }
  duckdb::idx_t GetSessionRoleId() const { return session_role; }

  std::string EffectiveUserName();
  std::string SessionUserName();

  void SetEffectiveRole(duckdb::idx_t role) {
    SetCurrentRole(role);
    session_user = EffectiveUserName();
    PublishSuperuser();
  }
  void SetSessionRole(duckdb::idx_t role) {
    session_role = role;
    SetEffectiveRole(role);
  }
  void ResetIdentity() {
    session_role = _login_role_id;
    SetEffectiveRole(_login_role_id);
  }
  void ApplyRoleSettings();

  auto* GetSendBuffer() const { return _send_buffer; }
  auto* GetSendGate() const { return _send_gate; }

  template<typename T>
  void SetSideChannel(T* value) {
    _side_channel.Set(value);
  }

  template<typename T>
  T* GetSideChannel() const {
    return _side_channel.Get<T>();
  }

  void SetInTransactionBlock(bool value) noexcept {
    _in_transaction_block = value;
  }
  void PreventInTransactionBlock(std::string_view statement) const;

  void AddNotice(irs::pg::SqlErrorData notice) {
    auto* node = new NoticeNode{std::move(notice), nullptr};
    node->next = _notices.load(std::memory_order_relaxed);
    while (!_notices.compare_exchange_weak(
      node->next, node, std::memory_order_release, std::memory_order_relaxed)) {
    }
  }

  bool HasNotices() const { return _notices.load(std::memory_order_relaxed); }

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
  void SetSettingChecked(std::string_view key, std::string value);
  void KeepSettingAsDefault(std::string_view key);

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
  void ReportIdle();
  absl::Mutex& SettingWritesMutex() noexcept { return _setting_writes; }

  int copy_stdin_open_count = 0;
  bool copy_stdin_done = false;

  std::shared_ptr<network::pg::WireSinkContext> wire_sink;

  pg::ProgressCommand pending_copy_command = pg::ProgressCommand::None;
  pg::ProgressIoType pending_copy_io = pg::ProgressIoType::None;
  duckdb::idx_t pending_copy_relid = 0;

  uint32_t job_depth = 0;

  std::unique_ptr<replication::OriginSession> replication_origin;

  void ValidateIsolationLevel(duckdb::TransactionIsolationLevel level) final;
  bool EmitWarning(const char* message, std::string_view subtype) final;
  void OnSettingChange(const std::string& name, duckdb::SetScope scope,
                       const duckdb::Value& previous) final;
  void OnQueryBegin() final;
  void OnQueryEnd() final;
  void OnTransactionPreCommit(duckdb::MetaTransaction& meta) final;
  void OnTransactionPreRollback(duckdb::MetaTransaction& meta) final;
  void OnTransactionCommit(duckdb::MetaTransaction& meta) final;
  void OnTransactionRollback(duckdb::MetaTransaction& meta) final;
  void OnBoundPlan(duckdb::Binder& binder, duckdb::LogicalOperator& plan) final;
  void OnStatementEnd() final {
    if (auto* transaction = CurrentTransaction()) {
      transaction->SearchTxn().CheckStatement(*this);
    }
  }
  duckdb::optional_ptr<duckdb::CopyOutputGate> GetCopyOutputGate(
    const std::string& file_path) final;

 private:
  struct NoticeNode {
    irs::pg::SqlErrorData data;
    NoticeNode* next;
  };

  void PublishSuperuser() {
    SetSetting("is_superuser", duckdb::Role(*this).is_superuser ? "on" : "off",
               false);
  }

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
  const duckdb::idx_t _database_id;
  const int32_t _backend_pid;
  network::CancelRegistry* const _cancel_registry;
  message::Buffer* const _send_buffer;
  network::SendGate* const _send_gate;
  const duckdb::idx_t _login_role_id;
  SideChannel _side_channel;
  const bool _owns_temporary_objects;
  bool _in_transaction_block = false;
  std::atomic<NoticeNode*> _notices{nullptr};
  std::shared_ptr<pg::ProgressSource> _progress;
  uint64_t _catalog_epoch = 0;
  uint64_t _settings_version = 0;
  irs::containers::NodeHashMap<std::string, TxnVariable> _variables;
  catalog::SessionSlot& _slot;
  absl::Mutex _transaction_mutex;
  absl::Mutex _setting_writes;
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
  duckdb::shared_ptr<duckdb::AttachedDatabase> temporary;
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
