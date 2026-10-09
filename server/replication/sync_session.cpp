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

#include "replication/sync_session.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/replication_lsn_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <utility>

#include "auth/role_closure.h"
#include "connector/duckdb_client_state.h"
#include "network/pg/wire_frames.h"
#include "pg/commands/create_subscription.h"
#include "pg/connection_context.h"
#include "pg/copy_in_bridge.h"
#include "pg/pg_types.h"
#include "pg/protocol.h"
#include "pg/sql_utils.h"
#include "replication/repl_source.h"

namespace sdb::replication {
namespace {

using network::pg::FrameKind;
using network::pg::FrameStatus;

}  // namespace

void CheckTargetColumns(std::string_view schema, std::string_view table,
                        const std::vector<std::string>& missing,
                        const std::vector<std::string>& generated) {
  const auto report = [&](const std::vector<std::string>& columns,
                          std::string_view what) {
    if (columns.empty()) {
      return;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("logical replication target relation \"", schema, ".", table,
              "\" ", what, columns.size() == 1 ? " column: " : " columns: ",
              absl::StrJoin(columns, ", ",
                            [](std::string* out, const std::string& column) {
                              absl::StrAppend(out, "\"", column, "\"");
                            })));
  };
  report(missing, "is missing replicated");
  report(generated, "has incompatible generated");
}

SyncSession::SyncSession(network::IoExecutor& exec, ReplicationTarget target,
                         size_t host_index)
  : PublisherSession{exec, target.conninfo, host_index,
                     target.subscription_name, target.require_password},
    _target{std::move(target)} {}

yaclib::Task<bool> SyncSession::RunJob(Job job) {
  _job_done.Reset();
  std::atomic_thread_fence(std::memory_order_seq_cst);
  if (_jobs_closed.load(std::memory_order_relaxed)) {
    co_return false;
  }
  _job.store(job, std::memory_order_release);
  this->_task->RequestRun();
  if (job == Job::Stream) {
    co_return true;
  }
  co_await _job_done.AwaitOn(*this->_ioexec);
  co_return _job_ok;
}

yaclib::Task<bool> SyncSession::SyncOne(SyncTable& table, uint64_t lsn,
                                        bool binary) {
  _sync_table = &table;
  _sync_lsn = lsn;
  if (!co_await RunJob(Job::Begin)) {
    co_await RunJob(Job::Rollback);
    co_return false;
  }
  if (!co_await CopyTable(table, binary)) {
    co_await RunJob(Job::Rollback);
    co_return false;
  }
  co_return co_await RunJob(Job::Commit);
}

yaclib::Task<bool> SyncSession::RunJobs() {
  for (;;) {
    auto job = _job.exchange(Job::None, std::memory_order_acq_rel);
    if (job == Job::None) {
      if (this->SendBroken()) {
        co_return false;
      }
      co_await this->_task->Park();
      continue;
    }
    if (job == Job::Stream) {
      co_return true;
    }
    _job_ok = co_await RunDuckJob(job);
    _job_done.Set();
  }
}

void SyncSession::FinishJobs() {
  if (_in_txn) {
    this->_txn_state->Rollback();
    _in_txn = false;
  }
  _job_ok = false;
  _jobs_closed.store(true, std::memory_order_relaxed);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  if (!_job_done.Ready()) {
    _job_done.Set();
  }
  this->_task->Finish();
}

yaclib::Task<bool> SyncSession::CopyTable(const SyncTable& table, bool binary) {
  std::string columns = absl::StrJoin(
    table.columns, ", ", [](std::string* out, const std::string& column) {
      out->append(pg::QuoteIdentifier(column));
    });
  const auto name = absl::StrCat(pg::QuoteIdentifier(table.schema), ".",
                                 pg::QuoteIdentifier(table.table));
  std::string query;
  if (table.row_filter || table.partitioned || table.generated) {
    query = absl::StrCat("COPY (SELECT ", columns, " FROM ",
                         table.partitioned ? "" : "ONLY ", name);
    if (table.row_filter) {
      absl::StrAppend(&query, " WHERE ", *table.row_filter);
    }
    query.append(") TO STDOUT");
  } else {
    query = absl::StrCat("COPY ", name, " (", columns, ") TO STDOUT");
  }
  if (binary) {
    query.append(" WITH (FORMAT binary)");
  }
  SendQuery(query);
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      Fail(ERRCODE_CONNECTION_FAILURE,
           "server closed the connection unexpectedly");
      co_return false;
    }
    const char type = frame.type;
    if (type == PQ_MSG_ERROR_RESPONSE) {
      Fail(network::pg::ParseErrorResponse(frame.payload));
      this->_frames.Consume(frame);
      co_await ReadUntilReady(nullptr);
      co_return false;
    }
    this->_frames.Consume(frame);
    if (type == PQ_MSG_COPY_OUT_RESPONSE) {
      break;
    }
  }
  sdb::pg::CopyInBridge bridge;
  this->_connection_ctx->SetSideChannel(&bridge);
  _copy_stmt =
    BuildCopyFromStdin(table.schema, table.table, table.columns, binary);
  _job_done.Reset();
  _job.store(Job::Copy, std::memory_order_release);
  this->_task->RequestRun();
  co_await this->RunCopyInFeeder(bridge, binary
                                           ? network::pg::CopyFormat::Binary
                                           : network::pg::CopyFormat::Text);
  co_await _job_done.AwaitOn(*this->_ioexec);
  this->_connection_ctx->SetSideChannel<sdb::pg::CopyInBridge>(nullptr);
  const bool copied = _job_ok;
  co_return co_await ReadUntilReady(nullptr) && copied;
}

bool SyncSession::SetupApplyConnection() {
  if (_target.database_oid == 0) {
    return false;
  }
  const std::string_view user =
    _target.owner_name.empty()
      ? std::string_view{irs::StaticStrings::kDefaultUser}
      : std::string_view{_target.owner_name};
  const duckdb::idx_t role =
    _target.owner_name.empty() ? pg::kRootUser : _target.owner_id;
  this->_conn = irs::DuckDBEngine::Instance().CreateConnection();
  duckdb::PhysicalSet::SetVariable(
    *this->_conn->context, duckdb::Identifier{"session_replication_role"},
    duckdb::SetScope::SESSION, duckdb::Value{"replica"});
  this->_txn_state.emplace(this->_conn->context->transaction);
  this->_connection_ctx = std::make_shared<ConnectionContext>(
    *this->_conn->context, user, role, _target.database_name,
    _target.database_oid, nullptr, 0, nullptr);
  this->_client_state = &connector::SereneDBClientState::Register(
    *this->_conn->context, this->_connection_ctx);
  this->_conn->context->session_user.assign(user);
  connector::SetDefaultSearchPath(*this->_conn->context, _target.database_name);
  return true;
}

void SyncSession::ApplyFailed(irs::pg::SqlErrorData error) {
  if (!_apply_error.errmsg.empty()) {
    return;
  }
  if (error.errcode == ERRCODE_T_R_SERIALIZATION_FAILURE) {
    _transient.store(true, std::memory_order_release);
  } else if (_target.disable_on_error) {
    _disable_requested.store(true, std::memory_order_release);
  }
  _apply_error = std::move(error);
}

yaclib::Task<bool> SyncSession::RunDuckJob(Job job) {
  try {
    switch (job) {
      case Job::Begin:
        co_return BeginSync();
      case Job::Copy:
        co_return co_await RunLocalCopy();
      case Job::Commit:
        co_return CommitSync();
      case Job::Rollback:
        if (_in_txn) {
          this->_txn_state->Rollback();
          _in_txn = false;
        }
        co_return true;
      case Job::None:
      case Job::Stream:
        break;
    }
  } catch (const std::exception& ex) {
    ApplyFailed(network::pg::ToSqlError(ex));
  }
  co_return false;
}

bool SyncSession::BeginSync() {
  this->_txn_state->Arm();
  _in_txn = true;
  auto& table = *_sync_table;
  auto local = LookupTable(table.schema, table.table);
  if (!local) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("logical replication target relation \"", table.schema, ".",
              table.table, "\" does not exist"));
  }
  std::vector<std::string> missing;
  std::vector<std::string> generated;
  for (const auto& name : table.columns) {
    const duckdb::ColumnDefinition* found = nullptr;
    for (const auto& column : local->GetColumns().Logical()) {
      if (column.Name().GetIdentifierName() == name) {
        found = &column;
        break;
      }
    }
    if (found == nullptr) {
      missing.push_back(name);
    } else if (found->Generated()) {
      generated.push_back(name);
    }
  }
  CheckTargetColumns(table.schema, table.table, missing, generated);
  table.owner = local->permissions.owner;
  return true;
}

yaclib::Task<bool> SyncSession::RunLocalCopy() {
  this->_client_state->copy_stdin_open_count = 0;
  this->_client_state->copy_stdin_done = false;
  auto* bridge = this->_connection_ctx->GetSideChannel<sdb::pg::CopyInBridge>();
  bool ok = false;
  try {
    UseRole(_sync_table->owner);
    auto prepared = this->_conn->Prepare(std::move(_copy_stmt));
    if (prepared->HasError()) {
      prepared->GetErrorObject().Throw();
    }
    ok = (co_await RunPrepared(*prepared)).has_value();
  } catch (const std::exception& ex) {
    ApplyFailed(network::pg::ToSqlError(ex));
  }
  if (!ok && bridge != nullptr) {
    bridge->Abort();
  }
  co_return ok;
}

bool SyncSession::CommitSync() {
  auto& context = *this->_conn->context;
  auto& catalog = duckdb::Catalog::GetCatalog(
    context, duckdb::Identifier{_target.database_name});
  const auto transaction = catalog.GetCatalogTransaction(context);
  auto entry = catalog.Cast<duckdb::DuckCatalog>().GetOidIndex().GetVisible(
    _target.subscription_oid, transaction.view);
  if (!entry || entry->type != duckdb::CatalogType::SUBSCRIPTION_ENTRY) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("subscription \"", _target.subscription_name,
                            "\" does not exist"));
  }
  duckdb::DuckTransaction::Get(context, catalog)
    .PushRelationSync(entry->Cast<duckdb::ReplicationLsnEntry>(),
                      _sync_table->sync_id, _sync_lsn);
  _in_txn = false;
  if (auto error = this->_txn_state->Commit()) {
    ApplyFailed(std::move(*error));
    return false;
  }
  for (auto& relation : _target.relations) {
    if (relation.sync_id == _sync_table->sync_id) {
      relation.state = 'r';
      relation.lsn = _sync_lsn;
    }
  }
  SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
           "' synchronized table \"", _sync_table->schema, ".",
           _sync_table->table, "\" at ", pg::FormatLsn(_sync_lsn));
  return true;
}

duckdb::optional_ptr<duckdb::TableCatalogEntry> SyncSession::LookupTable(
  std::string_view schema, std::string_view table) {
  return duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
    *this->_conn->context,
    duckdb::QualifiedName::FromCatalogSchema(
      duckdb::Identifier{_target.database_name}, {duckdb::Identifier{schema}},
      duckdb::Identifier{table}),
    duckdb::OnEntryNotFound::RETURN_NULL);
}

yaclib::Task<std::optional<int64_t>> SyncSession::RunPrepared(
  duckdb::PreparedStatement& prepared) {
  network::pg::ClosingPending pending;
  duckdb::vector<duckdb::Value> values;
  auto result =
    co_await this->DriveToResult(prepared, values, pending, nullptr);
  if (!result) {
    co_return std::nullopt;
  }
  if (result->HasError()) {
    result->ThrowError();
  }
  auto chunk = result->Fetch();
  if (!chunk || chunk->size() == 0 || chunk->ColumnCount() == 0) {
    co_return 0;
  }
  co_return chunk->GetValue(0, 0).GetValue<int64_t>();
}

void SyncSession::UseRole(duckdb::idx_t table_owner) {
  auto role = _target.owner_id;
  if (!_target.run_as_owner && table_owner != role &&
      table_owner != pg::kInvalidOid) {
    auto& context = *this->_conn->context;
    if (!auth::ClosureFor(&context, role)->is_superuser &&
        !auth::ClosureFor(&context, role)->CanSet(table_owner)) {
      const auto roles = auth::RolesOf(&context);
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("role \"", roles->NameOf(role), "\" cannot SET ROLE to \"",
                roles->NameOf(table_owner), "\""));
    }
    role = table_owner;
  }
  this->_connection_ctx->SetEffectiveRole(role);
}

TableSyncWorker::TableSyncWorker(network::IoExecutor& exec,
                                 ReplicationTarget target, SyncPlan& plan)
  : SyncSession{exec, std::move(target), 0}, _plan{plan} {}

void TableSyncWorker::Start(yaclib::Promise<bool> promise) {
  Run(std::move(promise)).Detach();
}

yaclib::Future<> TableSyncWorker::LocalMain() {
  co_await this->_task->Park();
  try {
    _setup_ok = SetupApplyConnection();
  } catch (const std::exception& ex) {
    ApplyFailed(network::pg::ToSqlError(ex));
  }
  _setup_done.Set();
  if (_setup_ok) {
    co_await RunJobs();
  }
  FinishJobs();
  co_return {};
}

yaclib::Task<> TableSyncWorker::Run(yaclib::Promise<bool> promise) {
  auto self = this->shared_from_this();
  auto writer = this->SendWriter();
  bool ok = co_await Connect();
  if (ok) {
    auto local = LocalMain();
    this->_handed_off = true;
    this->_task->Start();
    co_await _setup_done.AwaitOn(*this->_ioexec);
    ok = _setup_ok &&
         co_await Query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ") &&
         co_await Query(absl::StrCat("SET TRANSACTION SNAPSHOT ",
                                     pg::QuoteLiteral(_plan.snapshot)));
    while (ok) {
      const auto i = _plan.next.fetch_add(1, std::memory_order_relaxed);
      if (i >= _plan.tables.size()) {
        break;
      }
      ok = co_await SyncOne(_plan.tables[i], _plan.lsn, _plan.binary);
      if (ok) {
        _plan.done[i] = 1;
      }
    }
    ok = ok && co_await Query("COMMIT");
    co_await RunJob(Job::Stream);
    this->Stop();
    co_await std::move(local);
  }
  this->Stop();
  co_await std::move(writer);
  std::move(promise).Set(ok);
  co_return {};
}

}  // namespace sdb::replication
