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

#include "network/pg/pg_wire_session.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/escaping.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/transaction_statement.hpp>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/metrics.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/table.h"
#include "network/pg/bind_decoder.h"
#include "network/pg/copy_eod_scanner.h"
#include "network/pg/hba.h"
#include "network/pg/scram_messages.h"
#include "network/pg/startup_request.h"

namespace sdb::network::pg {
namespace {

// PG caps password/GSS auth tokens at PG_MAX_AUTH_TOKEN_LENGTH; a password
// message has no business being as large as a normal query frame (_max_message,
// 64 MiB), so cap auth frames here to bound the buffering an unauthenticated
// client can force.
inline constexpr uint32_t kMaxAuthToken = 65535;
// SCRAM exchange messages are tighter: PG caps each at
// PG_MAX_SASL_MESSAGE_LENGTH (real client-first/client-final are well under 1
// KiB).
inline constexpr uint32_t kMaxSaslMessage = 1024;

inline duckdb::LogicalType ResolveExpectedType(const auto& value_map,
                                               uint16_t id) {
  const auto it = value_map.find(duckdb::Identifier{absl::StrCat(id + 1)});
  if (it != value_map.end()) {
    const auto type = it->second->GetValue().type();
    if (type.id() != duckdb::LogicalTypeId::UNKNOWN &&
        type.id() != duckdb::LogicalTypeId::INVALID) {
      return type;
    }
  }
  return duckdb::LogicalTypeId::VARCHAR;
}

// Rethrow a DuckDB result's error iff it carries one. ErrorData::Throw()
// re-raises the original exception_ptr, so a serenedb SqlException
// (sqlstate/detail/hint) survives execution typed -- instead of being flattened
// through DuckErrorToSqlData, which only sees the bare message.
template<typename Result>
void ThrowIfError(Result& result) {
  if (result.HasError()) {
    result.GetErrorObject().Throw();
  }
}

// Rethrow a driven pending query's error iff the drive failed -- the executor
// signaled EXECUTION_ERROR, or the result captured one. Same typed rethrow as
// ThrowIfError; the single post-DriveQuery error boundary shared by the simple,
// extended, and COPY handlers.
inline void ThrowIfDriveFailed(duckdb::PendingQueryResult& pending,
                               duckdb::PendingExecutionResult status) {
  if (status == duckdb::PendingExecutionResult::EXECUTION_ERROR ||
      pending.HasError()) {
    pending.GetErrorObject().Throw();
  }
}

// Parse/Bind/Describe/Execute/Close/Flush. An error in one of these arms
// ignore-till-Sync; an error in simple Query (self-syncing) does not.
inline bool IsExtended(char type) {
  switch (type) {
    case PQ_MSG_PARSE:
    case PQ_MSG_BIND:
    case PQ_MSG_DESCRIBE:
    case PQ_MSG_EXECUTE:
    case PQ_MSG_CLOSE:
    case PQ_MSG_FLUSH:
      return true;
    default:
      return false;
  }
}

// COPY direction over a stdin/stdout pipe. The patched parser rewrites STDIN /
// STDOUT to /dev/stdin / /dev/stdout; a COPY to/from a real file classifies as
// None (it runs through the normal Prepare/Execute path).
enum class CopyDir : uint8_t { None, FromStdin, ToStdout };

struct CopyKind {
  CopyDir dir = CopyDir::None;
  CopyFormat format = CopyFormat::Other;
};

// Classified from the parsed statement BEFORE Prepare -- the CSV sniff opens
// /dev/stdin during Prepare, so the bridge must already be live by then.
inline CopyKind ClassifyCopy(duckdb::SQLStatement& statement) {
  if (statement.type != duckdb::StatementType::COPY_STATEMENT) {
    return {};
  }
  auto& copy = statement.Cast<duckdb::CopyStatement>();
  if (!copy.info) {
    return {};
  }
  auto& info = *copy.info;
  CopyFormat format = CopyFormat::Other;
  if (info.format == "binary") {
    format = CopyFormat::Binary;
  } else if (info.format == "text") {
    format = CopyFormat::Text;
  } else if (info.format == "parquet") {
    format = CopyFormat::Parquet;
  } else if (!info.is_from && info.format == "csv" &&
             !info.options.contains("header") &&
             !info.parsed_options.contains("header")) {
    // PostgreSQL writes no CSV header unless HEADER is given; DuckDB's CSV
    // writer writes one by default. Every COPY is classified here, so pin the
    // header off for COPY TO csv (covering both file and stdout); COPY FROM
    // keeps DuckDB's header auto-detection.
    info.options["header"] = {duckdb::Value::BOOLEAN(false)};
  }
  if (info.is_from) {
    return {info.file_path == "/dev/stdin" ? CopyDir::FromStdin : CopyDir::None,
            format};
  }
  return {info.file_path == "/dev/stdout" ? CopyDir::ToStdout : CopyDir::None,
          format};
}

// relid for pg_stat_progress_copy: `COPY table TO STDOUT` reports the table
// (explicit schema, else first search-path hit, as the binder resolves); the
// query form reports 0 (PG semantics).
inline ObjectId ResolveCopyTableId(ConnectionContext& conn,
                                   const duckdb::CopyInfo& info) {
  const auto& qname = info.GetQualifiedName();
  if (qname.Name().GetIdentifierName().empty()) {
    return {};
  }
  auto snapshot = conn.AcquireCatalogSnapshot();
  const auto db_id = conn.GetDatabaseId();
  std::shared_ptr<catalog::Table> table;
  if (!qname.Schema().empty()) {
    table = snapshot->GetTable(catalog::NoAccessCheck(), db_id,
                               qname.Schema().GetIdentifierName(),
                               qname.Name().GetIdentifierName());
  } else {
    for (const auto& schema : conn.GetSearchPath()) {
      table = snapshot->GetTable(catalog::NoAccessCheck(), db_id, schema,
                                 qname.Name().GetIdentifierName());
      if (table) {
        break;
      }
    }
  }
  return table ? table->GetId() : ObjectId{};
}

// Stage pg_stat_progress_copy classification for the statement about to run.
// Staged (not written to the metrics) because QueryBegin resets the metrics
// before applying it.
inline void StagePendingCopyProgress(connector::SereneDBClientState& state,
                                     ConnectionContext& conn,
                                     duckdb::SQLStatement& statement) {
  if (statement.type != duckdb::StatementType::COPY_STATEMENT) {
    return;
  }
  auto& copy = statement.Cast<duckdb::CopyStatement>();
  if (!copy.info) {
    return;
  }
  const auto& info = *copy.info;
  state.pending_copy_command = info.is_from ? sdb::pg::ProgressCommand::CopyFrom
                                            : sdb::pg::ProgressCommand::CopyTo;
  state.pending_copy_io =
    info.file_path == "/dev/stdin" || info.file_path == "/dev/stdout"
      ? sdb::pg::ProgressIoType::Pipe
      : sdb::pg::ProgressIoType::File;
  state.pending_copy_relid = ResolveCopyTableId(conn, info);
}

// A temporary CREATE -- e.g. a value-extracting PIVOT's bind-time enum, which
// the parser emits ahead of the SELECT (TEMPORARY + REPLACE_ON_CONFLICT,
// session scoped). Leading scaffolding like this is run at Parse and dropped
// from the body so the trailing statement prepares against it, keeping a PIVOT
// a single prepared statement. Non-temporary DDL (ALTER ADD COLUMN's expansion)
// is not.
inline bool IsTemporaryCreate(const duckdb::SQLStatement& statement) {
  return statement.type == duckdb::StatementType::CREATE_STATEMENT &&
         statement.Cast<duckdb::CreateStatement>().info->temporary;
}

bool IsCatalogDdl(duckdb::StatementType type) {
  using duckdb::StatementType;
  switch (type) {
    case StatementType::CREATE_STATEMENT:
    case StatementType::DROP_STATEMENT:
    case StatementType::ALTER_STATEMENT:
    case StatementType::ATTACH_STATEMENT:
    case StatementType::DETACH_STATEMENT:
      return true;
    default:
      return false;
  }
}

[[noreturn]] void ThrowAbortedTransaction() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                  ERR_MSG("current transaction is aborted, commands ignored "
                          "until end of transaction block"));
}

// Binary and text are the native PG COPY formats serenedb (de)serializes
// itself: TO STDOUT through the wire collector (parallel, zero-copy), FROM
// STDIN through the bridge. csv and the other DuckDB formats (json/parquet/...)
// go through DuckDB's own CopyFunction instead.
inline bool IsNativeCopyFormat(CopyFormat format) {
  return format == CopyFormat::Binary || format == CopyFormat::Text;
}

// The pg-wire CopyResponse overall-format byte (1 = binary, 0 = text): binary
// PGCOPY and parquet are binary; text/csv/json are text. New binary formats
// (excel/pdf/...) extend this one spot rather than scattered `== Parquet`
// tests.
inline bool IsBinaryWireFormat(CopyFormat format) {
  return format == CopyFormat::Binary || format == CopyFormat::Parquet;
}

inline void RejectBinaryCopyOptions(const duckdb::CopyInfo& info) {
  for (const auto& entry : info.options) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY option \"", entry.first,
                            "\" is not supported in BINARY mode"));
  }
  for (const auto& entry : info.parsed_options) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY option \"", entry.first,
                            "\" is not supported in BINARY mode"));
  }
}

// The runnable inner query for a COPY ... TO. `select_statement` is the parsed
// source SELECT for `COPY (query) TO`; for `COPY table TO` it is null until
// bind, so synthesize `SELECT [cols|*] FROM [catalog.][schema.]table` exactly
// as Binder::Bind(CopyStatement) does. Consumes the CopyStatement's nodes.
inline duckdb::unique_ptr<duckdb::SQLStatement> ExtractCopyToQuery(
  duckdb::CopyStatement& copy) {
  auto select = duckdb::make_uniq<duckdb::SelectStatement>();
  if (copy.info->select_statement) {
    select->node = std::move(copy.info->select_statement);
    return std::move(select);
  }
  auto ref = duckdb::make_uniq<duckdb::BaseTableRef>();
  const auto& qualified = copy.info->GetQualifiedName();
  ref->SetQualifiedName(qualified.Catalog(), qualified.Schema(),
                        qualified.Name());
  auto node = duckdb::make_uniq<duckdb::SelectNode>();
  node->from_table = std::move(ref);
  if (copy.info->select_list.empty()) {
    node->select_list.push_back(duckdb::make_uniq<duckdb::StarExpression>());
  } else {
    for (auto& name : copy.info->select_list) {
      node->select_list.push_back(
        duckdb::make_uniq<duckdb::ColumnRefExpression>(name));
    }
  }
  select->node = std::move(node);
  return std::move(select);
}

// The GUCs reported to the client via ParameterStatus: once at startup
// (SendStartupBurst) and again whenever a command changes one
// (ReportChangedParameters), matching postgres's GUC_REPORT set.
inline constexpr auto kReportParams = std::to_array<std::string_view>({
  "application_name",
  "client_encoding",
  "DateStyle",
  "default_transaction_read_only",
  "in_hot_standby",
  "integer_datetimes",
  "IntervalStyle",
  "is_superuser",
  "scram_iterations",
  "search_path",
  "server_encoding",
  "server_version",
  "session_authorization",
  "standard_conforming_strings",
  "TimeZone",
});

}  // namespace

// Startup-phase frames: RecvLoop is both byte producer and frame consumer,
// reading the socket directly.
template<SocketKind Kind>
yaclib::Task<Frame> PgWireSession<Kind>::NextFrame(FrameKind kind,
                                                   uint32_t max_len) {
  for (;;) {
    if (auto frame = _frames.TryAssemble(kind, max_len);
        frame.status != FrameStatus::NeedMore) {
      co_return frame;
    }
    auto [ec, n] =
      co_await this->_socket.ReadSome(this->_recv.Reserve(kReadBlock))
        .NoThrow();
    if (ec || n == 0) {
      co_return Frame{.status = FrameStatus::Malformed};
    }
    this->_recv.CommitWrite(n);
  }
}

// Command-phase frames: the SessionTask consumes what RecvLoop committed,
// parking until the next wake when the channel runs dry.
template<SocketKind Kind>
yaclib::Task<Frame> PgWireSession<Kind>::AwaitFrame(FrameKind kind,
                                                    uint32_t max_len) {
  for (;;) {
    if (auto frame = _frames.TryAssemble(kind, max_len);
        frame.status != FrameStatus::NeedMore) {
      co_return frame;
    }
    if (this->SendBroken()) {
      co_return Frame{.status = FrameStatus::Malformed};
    }
    co_await this->_task->Park();
  }
}

// COPY-feeder frames: same consumer role, woken via _copy_gate (the feeder is
// io-pinned and not a duckdb task).
template<SocketKind Kind>
yaclib::Task<Frame> PgWireSession<Kind>::FeedFrame(FrameKind kind,
                                                   uint32_t max_len) {
  for (;;) {
    if (auto frame = _frames.TryAssemble(kind, max_len);
        frame.status != FrameStatus::NeedMore) {
      co_return frame;
    }
    if (this->SendBroken()) {
      co_return Frame{.status = FrameStatus::Malformed};
    }
    co_await _copy_gate.Wait(*this->_ioexec);
  }
}

template<SocketKind Kind>
std::string_view PgWireSession<Kind>::DatabaseName() const {
  const auto it = _params.find("database");
  SDB_ASSERT(it != _params.end());
  return it->second;
}

template<SocketKind Kind>
std::string_view PgWireSession<Kind>::UserName() const {
  const auto it = _params.find("user");
  SDB_ASSERT(it != _params.end());
  return it->second;
}

template<SocketKind Kind>
bool PgWireSession<Kind>::SetupConnection() {
  const auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto database = snapshot->GetDatabase(DatabaseName());
  if (!database) {
    WriteFatalResponse(this->_send,
                       SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_CATALOG_NAME),
                                      ERR_MSG("database \"", DatabaseName(),
                                              "\" is not accessible")));
    return false;
  }
  const auto database_id = database->GetId();

  const std::string_view user = UserName();
  auto login = sdb::pg::RequireLoginRole(*snapshot, user, *database);
  if (!login.role) {
    WriteFatalResponse(this->_send, login.error);
    return false;
  }
  auto role = std::move(login.role);

  _conn = DuckDBEngine::Instance().CreateConnection();
  _txn_state.emplace(_conn->context->transaction);
  _connection_ctx = std::make_shared<ConnectionContext>(
    *_conn->context, user, role->GetId(), DatabaseName(), database_id,
    std::move(database), &this->_send, nullptr,
    static_cast<int32_t>(_cancel_key >> 32), _cancel);
  _client_state =
    &connector::SereneDBClientState::Register(*_conn->context, _connection_ctx);
  auto& client_config = duckdb::ClientConfig::GetConfig(*_conn->context);
  client_config.get_result_collector = MakeWireCollector;
  client_config.enable_progress_bar = true;
  client_config.print_progress_bar = false;
  // Fires per chunk on every write sink (INSERT/DELETE/UPDATE/MERGE and
  // COPY ... TO), so every write statement reports exact tuple/byte counts.
  client_config.sink_progress_callback =
    [state = _client_state](duckdb::idx_t rows, duckdb::idx_t bytes) {
      auto& metrics = state->Progress();
      sdb::pg::ProgressMetrics::Add(metrics.tuples_processed,
                                    static_cast<int64_t>(rows));
      sdb::pg::ProgressMetrics::Add(metrics.bytes_processed,
                                    static_cast<int64_t>(bytes));
      const auto command = static_cast<sdb::pg::ProgressCommand>(
        metrics.command.load(std::memory_order_relaxed));
      SDB_IF_FAILURE("pause_sst_sink_mid_copy") {
        if (command == sdb::pg::ProgressCommand::CopyFrom) {
          SDB_WAIT_ON_FAILURE("pause_sst_sink_mid_copy");
        }
      }
      SDB_IF_FAILURE("pause_copy_to_mid_stream") {
        if (command == sdb::pg::ProgressCommand::CopyTo) {
          SDB_WAIT_ON_FAILURE("pause_copy_to_mid_stream");
        }
      }
    };

  _conn->context->session_user = std::string{UserName()};
  std::vector<duckdb::CatalogSearchEntry> default_paths{
    duckdb::CatalogSearchEntry{duckdb::Identifier{DatabaseName()},
                               duckdb::Identifier{"$user"}},
    duckdb::CatalogSearchEntry{duckdb::Identifier{DatabaseName()},
                               duckdb::Identifier{"public"}},
  };
  _conn->context->client_data->catalog_search_path->SetDefaultPaths(
    std::vector{default_paths});
  _conn->context->client_data->catalog_search_path->Set(
    std::move(default_paths), duckdb::CatalogSetPathType::SET_DIRECTLY);

  _connection_ctx->SetSetting("session_authorization", std::string{UserName()},
                              false);
  _connection_ctx->SetSetting("is_superuser",
                              role->IsSuperuser() ? "on" : "off", false);

  for (const auto& [name, value] : _params) {
    // user/database are consumed by the connection identity; replication and
    // options are special startup parameters in PG (not GUCs) -- replication is
    // already handled (replication-mode connections are refused), and routing
    // either through the GUC layer wrongly rejects the whole connection.
    if (name == "user" || name == "database" || name == "replication" ||
        name == "options") {
      continue;
    }
    try {
      _connection_ctx->SetSettingChecked(name, value, false);
    } catch (const duckdb::InternalException&) {
      _connection_ctx->SetSetting(name, value, false);
    } catch (const std::exception& exception) {
      // A valid-but-non-UTF8 client_encoding (LATIN1, WIN1252, ...) is a normal
      // startup GUC every PG client may send; serenedb is UTF8-only and cannot
      // transcode, but refusing the whole connection over it is worse than PG,
      // which accepts it. Keep the session on UTF8 (already reported in the
      // startup burst) and warn the request was ignored.
      if (name == "client_encoding") {
        _connection_ctx->AddNotice(
          SQL_ERROR_DATA(ERR_CODE(ERRCODE_WARNING),
                         ERR_MSG("client_encoding \"", value,
                                 "\" is not supported; using \"UTF8\"")));
        continue;
      }
      // Prologue boundary: no command loop yet, so convert here and close.
      // Same ToSqlError funnel as the command loop -- keeps the real sqlstate /
      // detail / hint instead of forcing 22023 + raw what(). A startup GUC
      // failure aborts the connection, so PG reports it as FATAL, not ERROR.
      WriteFatalResponse(this->_send, ToSqlError(exception));
      return false;
    }
  }
  return true;
}

template<SocketKind Kind>
void PgWireSession<Kind>::SendStartupBurst() {
  static constexpr std::array<char, 9> kAuthOk{
    PQ_MSG_AUTHENTICATION_REQUEST, 0, 0, 0, 8, 0, 0, 0, 0};
  {
    message::Writer writer{this->_send};
    writer.Write({kAuthOk.data(), kAuthOk.size()});
    writer.Commit(false);
  }

  for (const auto name : kReportParams) {
    if (const auto value = _connection_ctx->Get(name)) {
      _reported_params.insert_or_assign(name, *value);
      WriteParameterStatus(this->_send, name, *value);
    }
  }
  // We just reported everything; subsequent polls only fire if a SET advances
  // the version past this point.
  _reported_settings_version = _connection_ctx->SettingsVersion();

  // BackendKeyData carries this session's cancel key (pid = high 32 bits,
  // secret = low 32; both 4 bytes for the 3.0 protocol). A CancelRequest echoes
  // them back so CancelRegistry can find and interrupt this connection.
  {
    message::Writer writer{this->_send};
    auto* backend_key = writer.Alloc(13);
    backend_key[0] = PQ_MSG_BACKEND_KEY_DATA;
    absl::big_endian::Store32(backend_key + 1, 12);
    absl::big_endian::Store32(backend_key + 5,
                              static_cast<uint32_t>(_cancel_key >> 32));
    absl::big_endian::Store32(backend_key + 9,
                              static_cast<uint32_t>(_cancel_key & 0xffffffffu));
    writer.Commit(false);
  }
  DrainNotices();
  WriteReadyForQuery(this->_send, 'I');
}

template<SocketKind Kind>
duckdb::unique_ptr<duckdb::PendingQueryResult>
PgWireSession<Kind>::PendingQueryEnsured(
  duckdb::PreparedStatement& prepared, duckdb::vector<duckdb::Value>& values,
  std::shared_ptr<WireSinkContext> wire) {
  _connection_ctx->AcquireCatalogSnapshot();
  if (prepared.GetStatementType() == duckdb::StatementType::COPY_STATEMENT &&
      prepared.data && prepared.data->unbound_statement) {
    StagePendingCopyProgress(*_client_state, *_connection_ctx,
                             *prepared.data->unbound_statement);
  }
  const bool is_ddl = IsCatalogDdl(prepared.GetStatementType());
  NoticeDdlInTransaction(prepared.GetStatementType());
  // Once an explicit transaction has run a snapshot-taking statement it can no
  // longer change its isolation level (SET TRANSACTION ISOLATION LEVEL must
  // precede any query -- enforced by isolation_level_validator). A statement
  // that reads or modifies a database takes a snapshot; SET/SHOW and
  // transaction-control statements do not.
  if (_connection_ctx->IsExplicitTransaction()) {
    const auto& props = prepared.GetStatementProperties();
    if (!props.read_databases.empty() || !props.modified_databases.empty()) {
      _connection_ctx->MarkQueryInTransaction();
    }
    // Classify the statement for the snapshot lifecycle (see Transaction).
    // Genuine DML pins all views; atomic DDL instead forces a catalog refresh
    // so a later statement (e.g. the backfill UPDATE of ALTER ADD COLUMN ...
    // DEFAULT) observes the new catalog, even under REPEATABLE READ. DuckDB
    // reports a modified database for DDL too, so the two are told apart here.
    if (is_ddl) {
      _connection_ctx->MarkStatementDdl();
    } else if (!props.modified_databases.empty()) {
      _connection_ctx->MarkStatementDml();
    }
  }
  if (!wire) {
    // Streaming can't engage here anyway: DDL/DML are FORCE_MATERIALIZED, and
    // the describe path reads only types/names without fetching. false keeps
    // result cleanup eager instead of leaving an open streaming result.
    return prepared.PendingQuery(values, /*allow_stream_result=*/false);
  }
  // Arm the collector hook for this execution. allow_stream_result=false
  // routes through the get_result_collector hook (only consulted when not
  // streaming) and gives the eager-cleanup materialized fetch path; the wire
  // collector streams the bytes itself. Snapshot must be set before sinks can
  // run (workers may pick tasks up during PendingQuery), so fill the
  // serialization template here, after CatalogSnapshot.
  FillContext(*_connection_ctx, wire->proto);
  _client_state->wire_sink = std::move(wire);
  auto pending = prepared.PendingQuery(values, /*allow_stream_result=*/false);
  _client_state->wire_sink.reset();
  return pending;
}

template<SocketKind Kind>
void PgWireSession<Kind>::NoticeDdlInTransaction(duckdb::StatementType type) {
  if (!IsCatalogDdl(type) || !_connection_ctx->IsExplicitTransaction()) {
    return;
  }
  if (_connection_ctx->GetStrictDDL()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
      ERR_MSG("DDL statements are not supported inside a transaction block: "
              "DDL commits immediately and cannot be rolled back "
              "(sdb_strict_ddl is enabled)"));
  }
  _connection_ctx->AddNotice(SQL_ERROR_DATA(
    ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
    ERR_MSG("DDL is not transactional: the statement commits immediately "
            "and is not undone by ROLLBACK")));
}

template<SocketKind Kind>
duckdb::unique_ptr<duckdb::PendingQueryResult>
PgWireSession<Kind>::PendingStatementEnsured(
  duckdb::unique_ptr<duckdb::SQLStatement> statement,
  const std::shared_ptr<WireSinkContext>& wire) {
  _connection_ctx->AcquireCatalogSnapshot();
  StagePendingCopyProgress(*_client_state, *_connection_ctx, *statement);
  // Statement type is parse-time, and the check must precede PendingQuery:
  // execution tasks (including the DDL itself) can run inside it.
  NoticeDdlInTransaction(statement->type);
  FillContext(*_connection_ctx, wire->proto);
  _client_state->wire_sink = wire;
  auto pending =
    _conn->PendingQuery(std::move(statement), /*allow_stream_result=*/false);
  _client_state->wire_sink.reset();
  if (pending && !pending->HasError() &&
      _connection_ctx->IsExplicitTransaction()) {
    // Post-bind statement classification; see PendingQueryEnsured. Consumed at
    // statement end, so running after PendingQuery (bind) is early enough.
    const auto& props = pending->properties;
    if (!props.read_databases.empty() || !props.modified_databases.empty()) {
      _connection_ctx->MarkQueryInTransaction();
    }
    if (IsCatalogDdl(pending->statement_type)) {
      _connection_ctx->MarkStatementDdl();
    } else if (!props.modified_databases.empty()) {
      _connection_ctx->MarkStatementDml();
    }
  }
  return pending;
}

template<SocketKind Kind>
yaclib::Task<duckdb::unique_ptr<duckdb::QueryResult>>
PgWireSession<Kind>::DriveStatementToResult(
  duckdb::unique_ptr<duckdb::SQLStatement> statement, ClosingPending& pending,
  std::shared_ptr<WireSinkContext> wire) {
  pending = PendingStatementEnsured(std::move(statement), wire);
  ThrowIfError(*pending);
  // The collector hook engages the context only for QUERY_RESULT plans;
  // everything else drives without the wire drain.
  const bool engaged = wire && wire->engaged;
  if (engaged) {
    this->ArmSendWaiter();
  }
  absl::Cleanup disarm = [this, engaged] {
    if (engaged) {
      this->DisarmSendWaiter();
    }
  };
  const auto status =
    co_await DriveQuery(*pending, engaged ? wire.get() : nullptr, false);
  ThrowIfDriveFailed(*pending, status);
  if (engaged) {
    co_await FinishWireDrain(*wire);
  }
  auto result = pending->Execute();
  ThrowIfError(*result);
  co_return std::move(result);
}

template<SocketKind Kind>
std::shared_ptr<WireSinkContext> PgWireSession<Kind>::MakeWireContext(
  std::span<const sdb::pg::VarFormat> formats) {
  // The collector's gstate co-owns the context, so a previous query torn down
  // late (error paths) may still hold a reference -- allocate fresh then.
  if (_wire_ctx && _wire_ctx.use_count() == 1) {
    _wire_ctx->Reset();
  } else {
    _wire_ctx = std::make_shared<WireSinkContext>();
    _wire_ctx->send = &this->_send;
    _wire_ctx->send_written = &this->_send_written;
    _wire_ctx->task = &*this->_task;
  }
  _wire_ctx->formats.assign(formats.begin(), formats.end());
  return _wire_ctx;
}

template<SocketKind Kind>
bool PgWireSession<Kind>::DrainWire(WireSinkContext& wire) {
  if (this->SendBroken()) {
    // The client is gone: discard the queue so blocked sinks can finish and the
    // query unwinds instead of wedging on backpressure.
    while (wire.PopChain()) {
    }
    wire.UnblockSinks(true, true);
    return false;
  }
  if (wire.mode != WireSinkContext::Mode::Direct) {
    // Parallel: splice queued chains into _send until it hits its own cap, then
    // release the workers once the queue is back under the bound. Workers gate
    // on the chain queue (they encode into their own lstate buffers); the send
    // cap only stops splicing.
    while (!this->OverSendHighWater()) {
      auto chain = wire.PopChain();
      if (!chain) {
        break;
      }
      this->_send.SpliceCommitted(std::move(*chain), false);
    }
    wire.UnblockSinks(wire.queued_bytes.load(std::memory_order_relaxed) <=
                      kSendHighWater);
    return true;
  }
  // Direct mode: the sink owns the _send producer role, so judge fullness by
  // its published progress, not the plain producer counter. The writer can be
  // ahead of direct_committed -- between paged Executes the session itself
  // appends (PortalSuspended) and the writer sends it -- so clamp the unsigned
  // delta instead of underflowing.
  const auto committed = wire.direct_committed.load(std::memory_order_acquire);
  const auto written = this->_send_written.load(std::memory_order_acquire);
  const auto unsent = committed > written ? committed - written : 0;
  // A paged sink parked at the row budget must stay parked until the next
  // Execute raises it; unblocking it here would just re-suspend (spin).
  const bool budget_ok =
    !wire.paged || wire.rows.load(std::memory_order_relaxed) <
                     wire.row_budget.load(std::memory_order_relaxed);
  if (unsent <= kSendHighWater && budget_ok) {
    wire.UnblockSinks(true, true);
  }
  return true;
}

template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::FinishWireDrain(WireSinkContext& wire) {
  // The send waiter is armed by the caller (DriveToResult) across the whole
  // drive, so this continues in the same armed span -- no re-arm here.
  for (;;) {
    if (!DrainWire(wire)) {
      co_return {};
    }
    if (wire.queued_bytes.load(std::memory_order_relaxed) == 0) {
      co_return {};
    }
    // Send buffer over the cap with chains still queued: wait for the writer.
    co_await this->_task->Park();
  }
}

template<SocketKind Kind>
auto PgWireSession<Kind>::FunnelError(const std::exception& exception,
                                      char type) -> PostError {
  // Notices accumulated before the failure go out ahead of the error, as PG
  // orders them (the post-dispatch drain would otherwise emit them after).
  DrainNotices();
  WriteErrorResponse(this->_send, ToSqlError(exception));
  // Roll back the implicit block inline (postgres aborts at the error, not at
  // the Sync): the Sync then finds nothing open -> status 'I'. If we don't own
  // a block but the user does, the explicit transaction is aborted instead so
  // it reads 'E' and subsequent statements surface 25P02.
  if (!RollbackImplicitBlock() && _txn_state->Explicit()) {
    _txn_state->AbortBlock();
  }
  return IsExtended(type) ? PostError::DiscardUntilSync : PostError::Resync;
}

template<SocketKind Kind>
void PgWireSession<Kind>::DrainNotices() {
  if (!_connection_ctx->HasNotices()) {
    return;
  }
  _connection_ctx->ConsumeNotices([this](const sdb::pg::SqlErrorData& notice) {
    WriteNoticeResponse(this->_send, notice);
  });
}

// A command may change a reportable GUC (e.g. SET TimeZone, SET search_path,
// or a transaction rollback reverting one). Postgres echoes such changes with
// ParameterStatus before the next ReadyForQuery; emit one for each value that
// differs from what the client was last told.
template<SocketKind Kind>
void PgWireSession<Kind>::ReportChangedParameters() {
  // Postgres reports a GUC only when it actually changed. A SET bumps the
  // connection's settings version; if nothing changed since our last report,
  // skip the (locked, per-GUC) poll entirely -- the common SELECT/DML path.
  const auto version = _connection_ctx->SettingsVersion();
  if (version == _reported_settings_version) {
    return;
  }
  _reported_settings_version = version;
  for (const auto name : kReportParams) {
    const auto value = _connection_ctx->Get(name);
    if (!value) {
      continue;
    }
    const auto it = _reported_params.find(name);
    if (it != _reported_params.end() && it->second == *value) {
      continue;
    }
    _reported_params.insert_or_assign(name, *value);
    WriteParameterStatus(this->_send, name, *value);
  }
}

// Commit our implicit block if we own it, and drop the txn-scoped portals.
// Clearing _open before the engine commit (inside ImplicitTxnState::Commit)
// means a commit failure still leaves the block resolved. Portals drop first
// (PG PreCommit_Portals): a Sync ends the implicit transaction, so every portal
// it owns -- even a suspended paged stream -- goes, and a later re-Execute
// fails like PG's 34000. Returns the commit error, if any, so the caller orders
// it against the CommandComplete.
template<SocketKind Kind>
std::optional<sdb::pg::SqlErrorData>
PgWireSession<Kind>::CommitImplicitBlock() {
  if (!_txn_state->ShouldCommitAtSync()) {
    return std::nullopt;
  }
  _proto.DropTxnPortals();
  return _txn_state->Commit();
}

template<SocketKind Kind>
bool PgWireSession<Kind>::RollbackImplicitBlock() {
  if (!_txn_state->ShouldRollback()) {
    return false;
  }
  _txn_state->Rollback();
  _proto.DropTxnPortals();  // PG AtCleanup_Portals: abort discards them
  return true;
}

template<SocketKind Kind>
void PgWireSession<Kind>::AfterTxnStatement() {
  // BEGIN hands the block to the user (the Sync must not resolve it);
  // COMMIT/ROLLBACK closes it, which drops the portals it owned.
  _txn_state->TrackTxnStatement();
  if (_txn_state->Closed()) {
    _proto.DropTxnPortals();
  }
}

template<SocketKind Kind>
void PgWireSession<Kind>::CommitAndReportReady() {
  // Commit our implicit block before reporting status, so autocommit is
  // restored and StatusByte reads 'I'. An errored batch already rolled back
  // inline; an explicit BEGIN is left open (the user owns it; status stays
  // 'T'/'E'). A commit failure reports the error but still emits ReadyForQuery.
  if (auto err = CommitImplicitBlock()) {
    WriteErrorResponse(this->_send, *err);
  }
  // Report GUC changes here, after the commit and immediately before
  // ReadyForQuery (PG's timing): the block commit reverts any SET LOCAL
  // overlay, so reporting afterwards tells the client the value it actually
  // keeps (and a set-then-reverted SET LOCAL nets to no message).
  ReportChangedParameters();
  WriteReadyForQuery(this->_send, _txn_state->StatusByte());
  this->KickSend();
}

// Row-returning results never come through here -- they flow via the wire
// collector or SerializePage; this only extracts the affected-row count and
// writes the tag.
template<SocketKind Kind>
void PgWireSession<Kind>::WriteCommandTag(
  const duckdb::PreparedStatement& prepared, duckdb::QueryResult& result,
  duckdb::StatementReturnType return_type) {
  uint64_t rows = 0;
  if (return_type == duckdb::StatementReturnType::CHANGED_ROWS) {
    auto chunk = result.FetchRaw();
    if (chunk && chunk->size() > 0) {
      rows = static_cast<uint64_t>(chunk->GetValue(0, 0).GetValue<int64_t>());
    }
  }
  WriteCommandComplete(this->_send, prepared, rows);
}

// Capture the client's peer IP once, before auth. Unix sockets and capture
// failures leave _peer_addr unspecified (no IP). A v4-mapped-v6 address is
// normalized back to IPv4 so HBA family-strict matching stays PG-faithful and
// is_loopback() sees the v4.
template<SocketKind Kind>
void PgWireSession<Kind>::CapturePeerAddress() {
  if constexpr (Kind == SocketKind::Unix) {
    return;  // no IP; the gate treats a unix peer as local via is_local
  } else {
    asio_ns::error_code ec;
    const auto endpoint = this->_socket.Lowest().remote_endpoint(ec);
    if (ec) {
      return;  // no usable peer: stays unspecified -> gate fails closed
    }
    const auto address = endpoint.address();
    if (address.is_v6()) {
      if (const auto v6 = address.to_v6(); v6.is_v4_mapped()) {
        _peer_addr = asio_ns::ip::make_address_v4(asio_ns::ip::v4_mapped, v6);
        return;
      }
    }
    _peer_addr = address;
  }
}

template<SocketKind Kind>
void PgWireSession<Kind>::WriteCommandTag(
  const sdb::pg::CommandTag& tag, duckdb::QueryResult& result,
  duckdb::StatementReturnType return_type) {
  uint64_t rows = 0;
  if (return_type == duckdb::StatementReturnType::CHANGED_ROWS) {
    auto chunk = result.FetchRaw();
    if (chunk && chunk->size() > 0) {
      rows = static_cast<uint64_t>(chunk->GetValue(0, 0).GetValue<int64_t>());
    }
  }
  WriteCommandComplete(this->_send, tag, rows);
}

template<SocketKind Kind>
yaclib::Task<bool> PgWireSession<Kind>::Authenticate() {
  CapturePeerAddress();

  // Consult the HBA ruleset first: it decides trust / reject / which method,
  // for every connection, regardless of whether a stored credential exists.
  const auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  const hba::MembershipFn is_member = [&snapshot](std::string_view user,
                                                  std::string_view group) {
    auto user_role = snapshot->GetRole(user);
    auto group_role = snapshot->GetRole(group);
    if (!user_role || !group_role) {
      return false;  // missing_ok: unknown login role or target group
    }
    // NOSUPER: explicit (direct/indirect) membership only -- the closure is the
    // membership set and does not implicitly include a superuser's non-members.
    const auto& closure = snapshot->ClosureFor(user_role->GetId());
    return std::ranges::binary_search(closure.closure, group_role->GetId());
  };

  hba::ClientInfo client;
  client.is_local = Kind == SocketKind::Unix;
  client.is_ssl = this->_socket.IsTls();
  client.is_gss = false;
  if (!_peer_addr.is_unspecified()) {
    if (_peer_addr.is_v4()) {
      client.family = AF_INET;
      const auto bytes = _peer_addr.to_v4().to_bytes();
      std::copy(bytes.begin(), bytes.end(), client.addr.begin());
    } else {
      client.family = AF_INET6;
      const auto bytes = _peer_addr.to_v6().to_bytes();
      std::copy(bytes.begin(), bytes.end(), client.addr.begin());
    }
  }  // else: unix / no-IP -> family 0, addr zeroed (ClientInfo defaults)
  client.is_replication = false;
  client.user = UserName();
  client.database = DatabaseName();

  const auto ruleset = hba::GetHbaRuleset();
  const hba::Decision decision = hba::Decide(*ruleset, client, is_member);

  using DKind = hba::Decision::Kind;
  hba::Method hba_method = hba::Method::ImplicitReject;
  switch (decision.kind) {
    case DKind::Trust:
      co_return true;  // SetupConnection still enforces CanLogin / role-exists
    case DKind::Reject:
      WriteFatalResponse(
        this->_send,
        SQL_ERROR_DATA(
          ERR_CODE(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
          ERR_MSG("no pg_hba.conf entry for host, user \"", UserName(),
                  "\", database \"", DatabaseName(), "\"")));
      co_return false;
    case DKind::Unsupported:
    case DKind::DeferredPeerIdent:
    case DKind::MatchedHostnameDeferred:
      WriteFatalResponse(
        this->_send,
        SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                       ERR_MSG(decision.reason)));
      co_return false;
    case DKind::Method:
      hba_method = decision.method;  // scram / md5 / password
      break;
  }

  // A password method matched: verify against the stored credential. A role
  // with no usable verifier fails, as in PostgreSQL. Passwordless trust is
  // expressed entirely in HBA (the superuser's loopback `trust` rules) and is
  // therefore already decided above, before this point.
  const auto credential =
    _credentials ? _credentials->LookupCredential(UserName()) : std::nullopt;
  if (!credential) {
    WriteFatalResponse(
      this->_send,
      SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_PASSWORD),
                     ERR_MSG("password authentication failed for user \"",
                             UserName(), "\"")));
    co_return false;
  }

  // Method (from HBA) x stored form, matching PostgreSQL:
  //  scram    : only a SCRAM secret works (md5 storage cannot do SCRAM).
  //  md5      : an md5 secret drives an md5 exchange; a SCRAM secret falls back
  //             to SCRAM (PG's md5-line-over-scram-password behavior); a
  //             cleartext (flag) secret is hashed to the md5 form.
  //  password : cleartext exchange verified against whatever is stored.
  bool ok = false;
  bool method_ok = true;
  if (hba_method == hba::Method::Scram) {
    if (credential->scram) {
      ok = co_await AuthenticateScram(*credential->scram);
    } else {
      method_ok = false;
    }
  } else if (hba_method == hba::Method::Md5) {
    if (credential->md5) {
      ok = co_await AuthenticateMd5(*credential->md5);
    } else if (credential->scram) {
      ok = co_await AuthenticateScram(*credential->scram);
    } else if (credential->cleartext) {
      ok = co_await AuthenticateMd5(
        BuildMd5Verifier(UserName(), *credential->cleartext));
    } else {
      method_ok = false;
    }
  } else if (hba_method == hba::Method::Password) {
    ok = co_await AuthenticateCleartext(*credential);
  } else {
    method_ok = false;
  }
  if (!method_ok) {
    WriteFatalResponse(
      this->_send,
      SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
        ERR_MSG("authentication method \"", hba::MethodName(hba_method),
                "\" cannot verify the stored password for user \"", UserName(),
                "\"")));
    co_return false;
  }
  if (!ok) {
    co_return false;
  }

  const auto login_role = snapshot->GetRole(UserName());
  if (login_role && login_role->HasValidUntil() &&
      duckdb::Timestamp::GetCurrentTimestamp().value >=
        login_role->ValidUntil()) {
    WriteFatalResponse(
      this->_send,
      SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_PASSWORD),
                     ERR_MSG("password authentication failed for user \"",
                             UserName(), "\"")));
    co_return false;
  }
  co_return true;
}

template<SocketKind Kind>
yaclib::Task<bool> PgWireSession<Kind>::AuthenticateCleartext(
  const Credential& credential) {
  if (!this->_socket.IsTls() && !_allow_cleartext) {
    WriteFatalResponse(
      this->_send,
      SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
        ERR_MSG("cleartext password authentication requires TLS")));
    co_return false;
  }
  WriteAuthRequest(this->_send, 3, {});  // AuthenticationCleartextPassword
  co_await this->Flush();
  auto frame = co_await NextFrame(FrameKind::Typed, kMaxAuthToken);
  if (frame.status != FrameStatus::Ok ||
      frame.type != PQ_MSG_PASSWORD_MESSAGE) {
    if (frame.status == FrameStatus::Ok) {
      WriteFatalResponse(
        this->_send,
        SQL_ERROR_DATA(
          ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
          ERR_MSG("expected password response, got message type ",
                  static_cast<int>(static_cast<unsigned char>(frame.type)))));
    }
    _frames.Consume(frame);
    co_return false;
  }
  auto payload = frame.payload;
  while (!payload.empty() && payload.back() == '\0') {
    payload.remove_suffix(1);
  }
  const std::string given{payload};
  _frames.Consume(frame);
  bool ok = false;
  if (credential.cleartext) {
    const std::string given_prepared = SaslPrep(given);
    const std::string want = SaslPrep(*credential.cleartext);
    ok = ConstantTimeEqual(
      {reinterpret_cast<const uint8_t*>(given_prepared.data()),
       given_prepared.size()},
      {reinterpret_cast<const uint8_t*>(want.data()), want.size()});
  } else if (credential.scram) {
    ok = VerifyCleartextAgainstScram(*credential.scram, given);
  } else if (credential.md5) {
    ok = VerifyCleartextAgainstMd5(*credential.md5, UserName(), given);
  }
  if (!ok) {
    WriteFatalResponse(
      this->_send,
      SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_PASSWORD),
                     ERR_MSG("password authentication failed for user \"",
                             UserName(), "\"")));
    co_return false;
  }
  co_return true;
}

template<SocketKind Kind>
yaclib::Task<bool> PgWireSession<Kind>::AuthenticateMd5(
  std::string stored_md5) {
  uint8_t salt[4];
  if (!RandomBytes(salt)) {
    WriteFatalResponse(this->_send,
                       SQL_ERROR_DATA(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                                      ERR_MSG("could not generate md5 salt")));
    co_return false;
  }
  WriteAuthRequest(
    this->_send, 5,
    std::string_view{reinterpret_cast<const char*>(salt), sizeof(salt)});
  co_await this->Flush();
  auto frame = co_await NextFrame(FrameKind::Typed, kMaxAuthToken);
  if (frame.status != FrameStatus::Ok ||
      frame.type != PQ_MSG_PASSWORD_MESSAGE) {
    if (frame.status == FrameStatus::Ok) {
      WriteFatalResponse(
        this->_send,
        SQL_ERROR_DATA(
          ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
          ERR_MSG("expected password response, got message type ",
                  static_cast<int>(static_cast<unsigned char>(frame.type)))));
    }
    _frames.Consume(frame);
    co_return false;
  }
  auto payload = frame.payload;
  while (!payload.empty() && payload.back() == '\0') {
    payload.remove_suffix(1);
  }
  const std::string given{payload};
  _frames.Consume(frame);
  const std::string want = BuildMd5Response(stored_md5, salt);
  const bool ok = ConstantTimeEqual(
    {reinterpret_cast<const uint8_t*>(given.data()), given.size()},
    {reinterpret_cast<const uint8_t*>(want.data()), want.size()});
  if (!ok) {
    WriteFatalResponse(
      this->_send,
      SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_PASSWORD),
                     ERR_MSG("password authentication failed for user \"",
                             UserName(), "\"")));
    co_return false;
  }
  co_return true;
}

template<SocketKind Kind>
yaclib::Task<bool> PgWireSession<Kind>::AuthenticateScram(
  const ScramVerifier& verifier) {
  const auto fail = [&](int code, auto&&... msg) {
    WriteFatalResponse(
      this->_send,
      SQL_ERROR_DATA(ERR_CODE(code),
                     ERR_MSG(std::forward<decltype(msg)>(msg)...)));
  };

  // Channel binding (-PLUS) requires TLS, so offer it only then.
  std::string mechanisms;
  if (this->_socket.IsTls()) {
    mechanisms = "SCRAM-SHA-256-PLUS";
    mechanisms.push_back('\0');
  }
  mechanisms += "SCRAM-SHA-256";
  mechanisms.push_back('\0');
  mechanisms.push_back('\0');
  WriteAuthRequest(this->_send, 10, mechanisms);  // AuthenticationSASL
  co_await this->Flush();

  // --- client-first (SASLInitialResponse) ---
  auto first = co_await NextFrame(FrameKind::Typed, kMaxSaslMessage);
  if (first.status != FrameStatus::Ok ||
      first.type != PQ_MSG_PASSWORD_MESSAGE) {
    if (first.status == FrameStatus::Ok) {
      fail(ERRCODE_PROTOCOL_VIOLATION,
           "expected SASL response, got message type ",
           static_cast<int>(static_cast<unsigned char>(first.type)));
    }
    _frames.Consume(first);
    co_return false;
  }
  std::string_view head = first.payload;
  const auto mech_end = head.find('\0');
  if (mech_end == std::string_view::npos ||
      head.size() < mech_end + 1 + sizeof(int32_t)) {
    _frames.Consume(first);
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SASL initial response");
    co_return false;
  }
  const std::string_view mechanism = head.substr(0, mech_end);
  const bool plus = mechanism == "SCRAM-SHA-256-PLUS";
  if (mechanism != "SCRAM-SHA-256" && !(plus && this->_socket.IsTls())) {
    _frames.Consume(first);
    fail(ERRCODE_PROTOCOL_VIOLATION, "unsupported SASL mechanism: ", mechanism);
    co_return false;
  }
  head.remove_prefix(mech_end + 1);
  const auto initial_len =
    static_cast<int32_t>(absl::big_endian::Load32(head.data()));
  head.remove_prefix(sizeof(int32_t));
  // The initial-response is the last field; its declared length must consume
  // exactly the rest of the frame (PG's pq_getmsgend rejects trailing bytes).
  if (initial_len < 0 || static_cast<size_t>(initial_len) != head.size()) {
    _frames.Consume(first);
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SASL initial response");
    co_return false;
  }
  const std::string client_first{head.substr(0, initial_len)};
  _frames.Consume(first);

  // parse gs2 header + client-first-bare ("n,," + "n=,r=<cnonce>")
  const auto parsed = ParseScramClientFirst(client_first);
  if (!parsed) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SCRAM message");
    co_return false;
  }
  const auto cbind_flag = parsed->cbind_flag;
  std::vector<uint8_t> cbind_data;
  if (cbind_flag.starts_with("p=")) {
    if (!plus || cbind_flag.substr(2) != "tls-server-end-point") {
      fail(ERRCODE_PROTOCOL_VIOLATION,
           "unsupported SCRAM channel binding type");
      co_return false;
    }
    cbind_data = this->_socket.ChannelBinding();
    if (cbind_data.empty()) {
      fail(ERRCODE_INTERNAL_ERROR, "channel binding data unavailable");
      co_return false;
    }
  } else if (cbind_flag == "n" || cbind_flag == "y") {
    if (plus) {
      fail(ERRCODE_PROTOCOL_VIOLATION,
           "SCRAM-SHA-256-PLUS selected without channel binding");
      co_return false;
    }
    if (cbind_flag == "y" && this->_socket.IsTls()) {
      // We advertised -PLUS, so a binding-capable client that declined it
      // signals a possible downgrade -- reject (RFC 5802 gs2-cbind-flag "y").
      fail(ERRCODE_PROTOCOL_VIOLATION, "channel binding required");
      co_return false;
    }
  } else {
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SCRAM channel binding flag");
    co_return false;
  }
  if (!parsed->authzid.empty()) {
    fail(ERRCODE_FEATURE_NOT_SUPPORTED, "authorization identity not supported");
    co_return false;
  }
  const std::string gs2_header{parsed->gs2_header};
  const std::string client_first_bare{parsed->client_first_bare};
  const auto r_pos = client_first_bare.find("r=");
  if (r_pos == std::string::npos) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SCRAM message");
    co_return false;
  }
  std::string_view client_nonce{client_first_bare};
  client_nonce.remove_prefix(r_pos + 2);
  if (const auto end = client_nonce.find(','); end != std::string_view::npos) {
    client_nonce = client_nonce.substr(0, end);
  }

  // --- server-first (SASLContinue) ---
  // 18 raw bytes base64-expand to the 24-char server portion appended to the
  // client nonce (RFC 5802 leaves the server-nonce length to the server).
  static constexpr size_t kScramServerNonceBytes = 18;
  uint8_t raw_nonce[kScramServerNonceBytes];
  if (!RandomBytes(raw_nonce)) {
    fail(ERRCODE_INTERNAL_ERROR, "could not generate nonce");
    co_return false;
  }
  const std::string combined_nonce = absl::StrCat(
    client_nonce, absl::Base64Escape({reinterpret_cast<const char*>(raw_nonce),
                                      sizeof(raw_nonce)}));
  const std::string server_first =
    absl::StrCat("r=", combined_nonce, ",s=", absl::Base64Escape(verifier.salt),
                 ",i=", verifier.iterations);
  WriteAuthRequest(this->_send, 11, server_first);
  co_await this->Flush();

  // --- client-final (SASLResponse) ---
  auto final_frame = co_await NextFrame(FrameKind::Typed, kMaxSaslMessage);
  if (final_frame.status != FrameStatus::Ok ||
      final_frame.type != PQ_MSG_PASSWORD_MESSAGE) {
    if (final_frame.status == FrameStatus::Ok) {
      fail(ERRCODE_PROTOCOL_VIOLATION,
           "expected SASL response, got message type ",
           static_cast<int>(static_cast<unsigned char>(final_frame.type)));
    }
    _frames.Consume(final_frame);
    co_return false;
  }
  const std::string client_final{final_frame.payload};
  _frames.Consume(final_frame);
  const auto parsed_final = ParseScramClientFinal(client_final);
  if (!parsed_final) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SCRAM message");
    co_return false;
  }
  const std::string_view without_proof = parsed_final->without_proof;
  const std::string_view proof_b64 = parsed_final->proof;
  const std::string_view c_value = parsed_final->channel_binding;
  const std::string_view r_value = parsed_final->nonce;
  if (r_value != combined_nonce) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "SCRAM nonce mismatch");
    co_return false;
  }
  std::string expected_cbind = gs2_header;
  expected_cbind.append(cbind_data.begin(), cbind_data.end());
  const auto cbind = Base64Decode(c_value);
  if (!cbind || *cbind != expected_cbind) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "SCRAM channel binding check failed");
    co_return false;
  }
  const auto proof = Base64Decode(proof_b64);
  if (!proof || proof->size() != static_cast<size_t>(kScramKeyLen)) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SCRAM proof");
    co_return false;
  }
  const std::string auth_message =
    absl::StrCat(client_first_bare, ",", server_first, ",", without_proof);
  if (!VerifyClientProof(verifier, auth_message, *proof)) {
    fail(ERRCODE_INVALID_PASSWORD, "password authentication failed for user \"",
         UserName(), "\"");
    co_return false;
  }
  const auto signature = ScramServerSignature(verifier, auth_message);
  WriteAuthRequest(
    this->_send, 12,
    absl::StrCat(
      "v=", absl::Base64Escape({reinterpret_cast<const char*>(signature.data()),
                                signature.size()})));
  co_return true;
}

template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::RunSimpleQuery(std::string_view query) {
  while (!query.empty() && query.back() == '\0') {
    query.remove_suffix(1);
  }
  if (query.empty()) {
    WriteEmptyFrame(this->_send, PQ_MSG_EMPTY_QUERY_RESPONSE);
    co_return {};
  }
  // Parsing binds too: the statement preprocessor resolves PRAGMA lookups
  // through the catalog (TryReparsePragma), so the snapshot must be held
  // before ExtractStatements.
  _connection_ctx->AcquireCatalogSnapshot();
  auto extracted = _conn->ExtractStatements(query);
  if (extracted.empty()) {
    // A non-empty but statement-less query (";", a bare comment): postgres
    // replies EmptyQueryResponse, not just a bare ReadyForQuery.
    WriteEmptyFrame(this->_send, PQ_MSG_EMPTY_QUERY_RESPONSE);
    co_return {};
  }
  // A multi-statement simple query is an implicit transaction block (postgres
  // exec_simple_query: use_implicit_block = list_length > 1): every statement
  // commits or rolls back together. A single statement stays autocommit -- the
  // common fast path. The self-sync at the command loop commits on success; a
  // statement error throws to the dispatch catch, which rolls the block back
  // inline. Arming happens lazily per statement (postgres re-evaluates each
  // iteration), so a leading or mid-string BEGIN/COMMIT/ROLLBACK reaches the
  // transaction operator itself rather than running wrapped in our block; a
  // following statement then arms the block (or joins the user's BEGIN).
  const bool implicit_block = extracted.size() > 1;
  // PG commits the implicit block in finish_xact_command BEFORE EndCommand for
  // the last statement (postgres.c:1304-1314), so a commit-time failure is
  // reported as an ErrorResponse in place of that statement's CommandComplete
  // -- never one then the other. Commit just before the final tag; the trailing
  // Sync then finds the block already resolved. A TRANSACTION_STATEMENT commits
  // itself, and a trailing COPY completes before reaching the tag, so both keep
  // the plain Sync-time commit.
  const auto commit_block_if_last = [&](bool is_last,
                                        duckdb::StatementType type) {
    if (is_last && implicit_block &&
        type != duckdb::StatementType::TRANSACTION_STATEMENT) {
      if (auto err = CommitImplicitBlock()) {
        throw sdb::SqlException{std::move(*err),
                                std::source_location::current()};
      }
    }
  };
  for (auto& statement : extracted) {
    const bool is_last = &statement == &extracted.back();
    // In an aborted transaction block PG rejects every statement except
    // COMMIT/ROLLBACK with 25P02 before parse-analysis. Mirror the extended
    // path (HandleParse): without this guard PREPARE/DEALLOCATE -- whose binder
    // permits an invalid transaction -- would execute here where PG rejects.
    if (!_txn_state->GuardNotAborted(*statement)) {
      ThrowAbortedTransaction();
    }
    if (implicit_block &&
        statement->type != duckdb::StatementType::TRANSACTION_STATEMENT) {
      _txn_state->Arm();
    }
    const auto copy = ClassifyCopy(*statement);
    if (copy.dir == CopyDir::FromStdin) {
      // Returns pinned to _ioexec; remaining statements just continue
      // inline-first from there.
      co_await RunCopyFromStdin(std::move(statement));
      continue;
    }
    if (copy.dir == CopyDir::ToStdout) {
      if (IsNativeCopyFormat(copy.format)) {
        // Inner query through the wire collector (parallel + PGCOPY/text
        // framing), instead of the single-threaded CopyFunction.
        co_await RunCopyToStdout(std::move(statement), copy.format);
      } else {
        // csv/json/parquet/...: DuckDB writes the bytes, the session frames.
        co_await RunCopyToStdoutViaFormat(std::move(statement), copy.format);
      }
      continue;
    }
    // Bind and execution share one duckdb query lifecycle (one transaction):
    // the plan's catalog references stay valid end-to-end. The command tag
    // needs the parse tree, so it is built before the statement moves; rows
    // go to the wire straight from the executor (wire_collector.h), with the
    // hook writing RowDescription post-bind, before any task can emit a
    // DataRow (a plan error then lands after T, which is postgres's
    // mid-stream error behavior).
    const auto stmt_type = statement->type;
    const auto tag = sdb::pg::BuildCommandTag(*statement, *_conn->context);
    auto wire = MakeWireContext({});
    wire->announce_rowdesc = true;
    ClosingPending pending;
    auto result =
      co_await DriveStatementToResult(std::move(statement), pending, wire);
    commit_block_if_last(is_last, stmt_type);
    DrainNotices();
    if (pending->properties.return_type ==
        duckdb::StatementReturnType::QUERY_RESULT) {
      WriteCommandComplete(this->_send, tag,
                           wire->rows.load(std::memory_order_relaxed));
      continue;
    }
    WriteCommandTag(tag, *result, pending->properties.return_type);
    if (stmt_type == duckdb::StatementType::TRANSACTION_STATEMENT) {
      AfterTxnStatement();
    }
  }
  co_return {};
}

template<SocketKind Kind>
yaclib::Task<duckdb::PendingExecutionResult> PgWireSession<Kind>::DriveQuery(
  duckdb::PendingQueryResult& pending, WireSinkContext* wire, bool own_waiter) {
  // Productive slices run back-to-back: a per-slice scheduler round-trip
  // costs an enqueue + futex wake per query, and the common short query
  // finishes on its second slice. The budget bounds worker hogging by long
  // CPU-bound queries.
  static constexpr int kInlineSliceBudget = 8;
  int inline_slices = 0;
  // Wire drives splice/unblock on every wake; direct-mode backpressure unblocks
  // ride writer progress, so their parks must receive FlushDone wakes. When the
  // caller owns the waiter (DriveToResult, spanning the following
  // FinishWireDrain too) we skip the self arm/disarm so the whole drive is one
  // armed span.
  const bool arm = own_waiter && wire != nullptr;
  if (arm) {
    this->ArmSendWaiter();
  }
  absl::Cleanup disarm = [this, arm] {
    if (arm) {
      this->DisarmSendWaiter();
    }
  };
  for (;;) {
    if (wire != nullptr) {
      DrainWire(*wire);
    }
    const auto status =
      pending.ExecuteTask([this] { this->_task->RequestRun(); });
    if (duckdb::PendingQueryResult::IsResultReady(status)) {
      co_return status;
    }
    // Paged portal: the Direct sink suspended at the row budget. Stop driving
    // (checked here, before any Park, so the sink's block-wake cannot be lost)
    // so the caller emits PortalSuspended; a re-Execute raises the budget and
    // resumes this same pending.
    if (wire != nullptr && wire->paged &&
        wire->rows.load(std::memory_order_relaxed) >=
          wire->row_budget.load(std::memory_order_relaxed)) {
      co_return status;
    }
    if (status == duckdb::PendingExecutionResult::RESULT_NOT_READY) {
      if (++inline_slices < kInlineSliceBudget) {
        continue;
      }
      inline_slices = 0;
      co_await this->_task->Yield();
    } else {
      // NO_TASKS / BLOCKED: the pool is executing the query (or it is
      // blocked); the executor's on_reschedule wake re-runs us.
      inline_slices = 0;
      co_await this->_task->Park();
    }
  }
}

template<SocketKind Kind>
yaclib::Task<duckdb::unique_ptr<duckdb::QueryResult>>
PgWireSession<Kind>::DriveToResult(
  duckdb::PreparedStatement& prepared, duckdb::vector<duckdb::Value>& values,
  ClosingPending& pending, std::shared_ptr<WireSinkContext> wire,
  std::shared_ptr<const catalog::Snapshot> bound_snapshot) {
  // bound_snapshot pins the catalog snapshot the plan was bound against for
  // the duration of this frame -- the whole drive runs inside it, and the
  // bound entries live in snapshot-owned materializations. Reads still go
  // through the freshly acquired snapshot (cached plans see current catalog
  // data per execution); named statements pin via Statement::BindSnapshot()
  // instead, which spans portal re-Executes.
  pending = PendingQueryEnsured(prepared, values, wire);
  ThrowIfError(*pending);
  // One armed span over the whole drive (DriveQuery + the FinishWireDrain
  // splice loop): the gap between them never parks, so a single arm/disarm
  // replaces the per-call pair each would otherwise do.
  const bool arm = static_cast<bool>(wire);
  if (arm) {
    this->ArmSendWaiter();
  }
  absl::Cleanup disarm = [this, arm] {
    if (arm) {
      this->DisarmSendWaiter();
    }
  };
  const auto status = co_await DriveQuery(*pending, wire.get(), false);
  // Partial rows may already be on the wire; the error response after them is
  // exactly postgres's mid-stream error behavior.
  ThrowIfDriveFailed(*pending, status);
  if (wire) {
    co_await FinishWireDrain(*wire);
  }
  auto result = pending->Execute();
  ThrowIfError(*result);
  co_return std::move(result);
}

// COPY FROM STDIN binds (sniffs the CSV dialect by reading /dev/stdin) at
// PendingQuery time, so the bridge + feeder must be live BEFORE binding, and
// CopyInResponse must already be on its way out (the client streams CopyData
// only after seeing it). While the COPY runs, the io-pinned feeder takes over
// the recv-consumer role: it assembles CopyData frames from the channel and
// publishes them to the bridge, whose worker-side Read blocks inside DuckDB's
// synchronous FileSystem::Read; the SessionTask itself is parked in DriveQuery
// for the duration, so the single-consumer invariant holds.
template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::RunCopyFromStdin(
  duckdb::unique_ptr<duckdb::SQLStatement> statement) {
  const auto format =
    statement ? ClassifyCopy(*statement).format : CopyFormat::Other;
  // Consume the command frame early so the feeder starts past it, and hand
  // the recv-consumer role over BEFORE the client can send CopyData.
  if (_dispatch_consume) {
    this->_recv.Consume(_dispatch_consume);
    _dispatch_consume = 0;
  }
  // parquet needs a seekable file (footer at EOF); it cannot be read from a
  // non-seekable STDIN pipe. Reject here -- before CopyInResponse -- so the
  // client gets a clear error instead of a deep filesystem failure mid-COPY.
  if (format == CopyFormat::Parquet) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("COPY FROM STDIN (FORMAT parquet) is not supported: "
              "parquet requires a regular file"));
  }
  _client_state->copy_stdin_open_count = 0;
  _client_state->copy_stdin_done = false;
  sdb::pg::CopyInBridge bridge;
  _connection_ctx->SetCopyInBridge(&bridge);
  _feeder_done.store(false, std::memory_order_relaxed);
  _copy_route.store(true, std::memory_order_release);
  // CopyInResponse's column count: the explicit COPY column list, else the
  // target table's column count (PG sends natts + one per-column format code).
  // No DuckDB transaction is active yet (Prepare binds below), so read the
  // count from serenedb's catalog snapshot, not duckdb::Catalog.
  const auto& copy_info = *statement->Cast<duckdb::CopyStatement>().info;
  if (format == CopyFormat::Binary) {
    RejectBinaryCopyOptions(copy_info);
  }
  auto copy_columns = static_cast<int16_t>(copy_info.select_list.size());
  if (copy_columns == 0) {
    auto snapshot = _connection_ctx->AcquireCatalogSnapshot();
    const auto db_id = _connection_ctx->GetDatabaseId();
    std::shared_ptr<catalog::Table> table;
    const auto& copy_name = copy_info.GetQualifiedName();
    if (!copy_name.Schema().empty()) {
      table = snapshot->GetTable(catalog::NoAccessCheck(), db_id,
                                 copy_name.Schema().GetIdentifierName(),
                                 copy_name.Name().GetIdentifierName());
    } else {
      // Unqualified target: resolve across the search path by presence (the
      // schema that CONTAINS the table, as the binder does) -- the current
      // schema alone would miss a table in a later search-path schema.
      for (const auto& schema : _connection_ctx->GetSearchPath()) {
        table = snapshot->GetTable(catalog::NoAccessCheck(), db_id, schema,
                                   copy_name.Name().GetIdentifierName());
        if (table) {
          break;
        }
      }
    }
    if (table) {
      copy_columns = static_cast<int16_t>(table->Columns().size());
    }
  }
  WriteCopyInResponse(this->_send, IsBinaryWireFormat(format), copy_columns);
  this->KickSend();
  RunCopyInFeeder(bridge, format).Detach();

  // The feeder join below is an async park (co_await), so the cleanup that must
  // run on every exit cannot live in a destructor (no RAII guard /
  // absl::Cleanup here). Stash any error, run the async join, then re-propagate
  // to the command-loop boundary -- this is the reason for the exception_ptr,
  // not laziness.
  std::exception_ptr error;
  duckdb::unique_ptr<duckdb::QueryResult> result;
  duckdb::StatementReturnType return_type{};
  const auto tag = sdb::pg::BuildCommandTag(*statement, *_conn->context);
  try {
    // COPY prepare now is defer to execution.
    auto wire = MakeWireContext({});
    ClosingPending pending;
    result =
      co_await DriveStatementToResult(std::move(statement), pending, wire);
    return_type = pending->properties.return_type;
  } catch (...) {
    error = std::current_exception();
  }
  // Always join the feeder before the bridge (a local) is destroyed; Abort
  // wakes it if the worker errored without draining the client's CopyData.
  // The join is a park (the feeder's last act is RequestRun), keeping the
  // SessionTask on its pool.
  bridge.Abort();
  _copy_gate.Kick();
  while (!_feeder_done.load(std::memory_order_acquire)) {
    co_await this->_task->Park();
  }
  _copy_route.store(false, std::memory_order_release);
  _connection_ctx->SetCopyInBridge(nullptr);
  if (error) {
    std::rethrow_exception(error);
  }
  WriteCommandTag(tag, *result, return_type);
  co_return {};
}

// COPY ... TO STDOUT (FORMAT binary|text): runs the COPY's inner query through
// the SAME wire collector SELECT uses (parallel + zero-copy + SELECT ordering),
// but with the collector framing rows as PGCOPY/PG-text CopyData instead of
// DataRow. The session brackets the stream: CopyOutResponse (+ the PGCOPY
// header for binary) before PendingQuery (arming hands the _send producer role
// to the direct-mode sink, and workers may run it inside PendingQuery -- so the
// header must already be committed), the trailer (binary only) + CopyDone + the
// COPY-N tag after the drive.
template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::RunCopyToStdout(
  duckdb::unique_ptr<duckdb::SQLStatement> statement, CopyFormat format) {
  // Only the native PG formats reach here; binary is PGCOPY, text is COPY text.
  const bool binary = format == CopyFormat::Binary;
  auto& copy = statement->Cast<duckdb::CopyStatement>();
  if (binary) {
    RejectBinaryCopyOptions(*copy.info);
  }
  // The wire collector runs the extracted INNER query, so the generic COPY
  // staging in PendingQueryEnsured never sees this statement.
  StagePendingCopyProgress(*_client_state, *_connection_ctx, *statement);
  auto inner = ExtractCopyToQuery(copy);
  auto bind_snapshot = _connection_ctx->AcquireCatalogSnapshot();
  auto prepared = _conn->Prepare(std::move(inner));
  ThrowIfError(*prepared);

  // CopyOutResponse, plus (binary only) the PGCOPY 19-byte header as a CopyData
  // frame, both before arming/PendingQuery (see comment above).
  WriteCopyOutResponse(this->_send, binary,
                       static_cast<int16_t>(prepared->ColumnCount()));
  if (binary) {
    WriteCopyBinaryHeader(this->_send);
  }

  static constexpr std::array<sdb::pg::VarFormat, 1> kAllBinary{
    sdb::pg::VarFormat::Binary};
  static constexpr std::array<sdb::pg::VarFormat, 1> kAllCopyText{
    sdb::pg::VarFormat::CopyText};
  auto wire =
    MakeWireContext(binary ? std::span<const sdb::pg::VarFormat>{kAllBinary}
                           : std::span<const sdb::pg::VarFormat>{kAllCopyText});
  wire->row_encoding = binary ? RowEncoding::CopyBinary : RowEncoding::CopyText;
  if (!binary) {
    // COPY text fields escape inline through the reused Text serializers
    // (SerializationContext::copy_text); backslash_count = 2 is the COPY
    // base (a literal backslash -> \\), which array/record depth doubling
    // composes on top of.
    wire->proto.copy_text = true;
    wire->proto.backslash_count = 2;
    // The parser splits COPY options: value-form options (DELIMITER '|') land
    // in info->options, while NULL / HEADER / FORCE_* land in parsed_options.
    // Consult both before bind so every text option (and the csv-only
    // rejection) takes effect on the pre-bind STDOUT path.
    auto opts =
      sdb::connector::ResolveTextCopyOptions(copy.info->parsed_options);
    sdb::connector::ResolveTextCopyOptions(copy.info->options, opts);
    wire->proto.copy_delim = opts.delim;
    wire->proto.copy_null = opts.null_str;
    // PG-compat: COPY ... TO STDOUT (FORMAT text, HEADER) emits the column-name
    // header line before the rows (committed here, before the drive arms).
    if (opts.header) {
      WriteCopyTextHeader(this->_send, prepared->GetNames(), opts.delim);
    }
  }
  duckdb::vector<duckdb::Value> params;
  ClosingPending pending;
  co_await DriveToResult(*prepared, params, pending, wire,
                         std::move(bind_snapshot));

  // Binary: trailer (int16 -1 CopyData frame). Both: CopyDone closes the
  // stream, then the COPY N tag (postgres reports COPY, not SELECT, for COPY TO
  // STDOUT).
  if (binary) {
    WriteCopyBinaryFooter(this->_send);
  }
  WriteCopyDone(this->_send);
  WriteCommandComplete(
    this->_send,
    sdb::pg::CommandTag{"COPY", duckdb::StatementType::COPY_STATEMENT, true},
    wire->rows.load(std::memory_order_relaxed));
  co_return {};
}

// COPY ... TO STDOUT for the DuckDB-native formats (csv / json / parquet /
// ...): DuckDB's CopyFunction produces the format bytes into the dumb wire sink
// (CopyOutFileHandle -> CopyData), and the session owns the wire framing -- one
// path for every format, so we never reimplement a format writer. natts for
// CopyOutResponse is the COPY source's column count, taken from a prepared
// clone of the source (preparing the real COPY consumes the statement). The
// COPY (incl. its copy-function bind) is validated BEFORE CopyOutResponse, so a
// bind error stays a plain ErrorResponse with no half-open COPY stream; and
// CopyDone is emitted only after a successful drive (an error throws to the
// funnel).
template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::RunCopyToStdoutViaFormat(
  duckdb::unique_ptr<duckdb::SQLStatement> statement, CopyFormat format) {
  auto probe = statement->Copy();
  auto bind_snapshot = _connection_ctx->AcquireCatalogSnapshot();
  auto prepared = _conn->Prepare(std::move(statement));
  ThrowIfError(*prepared);
  auto inner = ExtractCopyToQuery(probe->Cast<duckdb::CopyStatement>());
  _connection_ctx->AcquireCatalogSnapshot();
  auto inner_prepared = _conn->Prepare(std::move(inner));
  ThrowIfError(*inner_prepared);

  WriteCopyOutResponse(this->_send, IsBinaryWireFormat(format),
                       static_cast<int16_t>(inner_prepared->ColumnCount()));
  duckdb::vector<duckdb::Value> params;
  ClosingPending pending;
  auto result = co_await DriveToResult(*prepared, params, pending, nullptr,
                                       std::move(bind_snapshot));
  WriteCopyDone(this->_send);
  WriteCommandTag(*prepared, *result,
                  prepared->GetStatementProperties().return_type);
  co_return {};
}

template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::RunCopyInFeeder(
  sdb::pg::CopyInBridge& bridge, CopyFormat format) {
  // Only the PG text format has the "\." end-of-data marker to scan for; binary
  // and csv stream straight through.
  const bool is_text = format == CopyFormat::Text;
  absl::Cleanup done_guard = [this] {
    _feeder_done.store(true, std::memory_order_release);
    this->_task->RequestRun();
  };
  const auto fail = [&](int code, auto&&... msg) {
    bridge.Fail(std::make_exception_ptr(sdb::SqlException{
      SQL_ERROR_DATA(ERR_CODE(code),
                     ERR_MSG(std::forward<decltype(msg)>(msg)...)),
      std::source_location::current()}));
  };
  // CopyData bodies stream straight to the bridge -- never assembled whole, so
  // there is no per-message size cap (PG/pgwire-rs accept ~1GB; memory stays
  // bounded by the recv buffer, not the frame length). The text "\."
  // end-of-data marker is recognised by a stateful scanner that survives
  // chunk/frame splits; binary/CSV pass through untouched.
  CopyEodScanner scanner;
  bool eod = false;  // text marker seen: stop feeding, just drain to CopyDone
  constexpr size_t kHeader = 1 + sizeof(uint32_t);
  for (;;) {
    while (this->_recv.ReadableSize() < kHeader) {
      if (this->SendBroken()) {
        fail(ERRCODE_CONNECTION_EXCEPTION,
             "unexpected EOF during COPY from stdin");
        co_return {};
      }
      co_await _copy_gate.Wait(*this->_ioexec);
    }
    std::array<uint8_t, kHeader> head;
    {
      size_t off = 0;
      for (const auto buffer : this->_recv.ReadableView(kHeader)) {
        std::memcpy(head.data() + off, buffer.data(), buffer.size());
        off += buffer.size();
      }
    }
    const char type = static_cast<char>(head[0]);
    const uint32_t length = absl::big_endian::Load32(head.data() + 1);
    if (length < sizeof(uint32_t)) {
      fail(ERRCODE_PROTOCOL_VIOLATION, "invalid COPY message length");
      co_return {};
    }
    this->_recv.Consume(kHeader);
    uint64_t body = length - sizeof(uint32_t);

    if (type == PQ_MSG_COPY_DATA) {
      while (body > 0) {
        while (!this->_recv.Readable()) {
          if (this->SendBroken()) {
            fail(ERRCODE_CONNECTION_EXCEPTION,
                 "unexpected EOF during COPY from stdin");
            co_return {};
          }
          co_await _copy_gate.Wait(*this->_ioexec);
        }
        const std::string_view chunk = this->_recv.Front();
        const auto take = std::min<uint64_t>(body, chunk.size());
        if (!eod && !bridge.Aborted()) {
          const std::string_view piece = chunk.substr(0, take);
          std::array<std::string_view, 2> spans{piece, std::string_view{}};
          if (is_text) {
            const auto scanned = scanner.Scan(piece);
            spans = {scanned.carry, scanned.data};
          }
          for (const auto span : spans) {
            if (span.empty()) {
              continue;
            }
            bridge.Publish(span.data(), span.size());
            co_await bridge.Drained(*this->_ioexec);
            bridge.ResetDrained();
          }
          if (is_text && scanner.Ended()) {
            eod = true;
            // worker sees EOF; the rest of the stream is dropped
            bridge.Finish();
          }
        }
        this->_recv.Consume(take);
        body -= take;
      }
    } else if (type == PQ_MSG_COPY_DONE) {
      if (!eod && !bridge.Aborted()) {
        if (is_text) {
          if (const auto tail = scanner.Finish(); !tail.empty()) {
            bridge.Publish(tail.data(), tail.size());
            co_await bridge.Drained(*this->_ioexec);
            bridge.ResetDrained();
          }
        }
        bridge.Finish();
      }
      co_return {};
    } else if (type == PQ_MSG_COPY_FAIL) {
      // The client's CopyFail carries its own failure text; PG echoes it,
      // reading it as a NUL-terminated string (truncate at the first NUL).
      std::string detail;
      detail.reserve(body);
      while (detail.size() < body) {
        while (!this->_recv.Readable()) {
          if (this->SendBroken()) {
            fail(ERRCODE_CONNECTION_EXCEPTION,
                 "unexpected EOF during COPY from stdin");
            co_return {};
          }
          co_await _copy_gate.Wait(*this->_ioexec);
        }
        const std::string_view chunk = this->_recv.Front();
        const auto take =
          std::min<uint64_t>(body - detail.size(), chunk.size());
        detail.append(chunk.data(), take);
        this->_recv.Consume(take);
      }
      fail(ERRCODE_QUERY_CANCELED, "COPY from stdin failed: ",
           std::string_view{detail}.substr(0, detail.find('\0')));
      co_return {};
    } else if (type != PQ_MSG_FLUSH && type != PQ_MSG_SYNC) {
      while (body > 0) {
        while (!this->_recv.Readable()) {
          if (this->SendBroken()) {
            fail(ERRCODE_CONNECTION_EXCEPTION,
                 "unexpected EOF during COPY from stdin");
            co_return {};
          }
          co_await _copy_gate.Wait(*this->_ioexec);
        }
        const auto take = std::min<uint64_t>(body, this->_recv.Front().size());
        this->_recv.Consume(take);
        body -= take;
      }
      fail(ERRCODE_PROTOCOL_VIOLATION,
           "unexpected message during COPY from stdin");
      co_return {};
    }
    // Flush/Sync carry an empty body -- already fully consumed (header only).
  }
}

template<SocketKind Kind>
void PgWireSession<Kind>::HandleParse(std::string_view payload) {
  const auto name_end = payload.find('\0');
  const auto query_end = name_end == std::string_view::npos
                           ? std::string_view::npos
                           : payload.find('\0', name_end + 1);
  if (query_end == std::string_view::npos ||
      payload.size() - query_end - 1 < sizeof(uint16_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Parse message"));
  }
  const std::string_view statement_name = payload.substr(0, name_end);
  const std::string_view query =
    payload.substr(name_end + 1, query_end - name_end - 1);
  payload.remove_prefix(query_end + 1);
  const auto num_params = absl::big_endian::Load16(payload.data());
  payload.remove_prefix(sizeof(uint16_t));
  if (payload.size() < num_params * sizeof(int32_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Parse message"));
  }

  duckdb::case_insensitive_map_t<duckdb::LogicalType> type_hints;
  if (num_params > 0) {
    const auto snapshot = _connection_ctx->AcquireCatalogSnapshot();
    for (uint16_t i = 0; i < num_params; ++i) {
      const auto oid =
        static_cast<int32_t>(absl::big_endian::Load32(payload.data()));
      payload.remove_prefix(sizeof(int32_t));
      if (oid != 0) {
        type_hints.emplace(absl::StrCat(i + 1),
                           sdb::pg::Oid2Type(oid, *snapshot));
      }
    }
  }

  StatementPtr* slot = &_proto.statements.Anon();
  if (statement_name.empty()) {
    // PG drops the prior unnamed statement before parsing, so a failed re-Parse
    // leaves no usable unnamed statement; a portal already bound from the prior
    // one keeps its own plan (it copied the StatementPtr at Bind).
    *slot = std::make_shared<Statement>();
  }
  // Parse once and branch on the parsed statement (no text pre-scan). COPY ...
  // FROM STDIN can't be bound here -- the CSV sniff would open /dev/stdin
  // before the CopyData feeder is live -- so the unbound statement is stashed
  // and bound at Execute via RunCopyFromStdin. Everything else binds the
  // already-parsed statement now, so the common path parses once.
  // wrap_multi=false: a single user command the parser expands into several
  // statements (ALTER ... ADD COLUMN ... DEFAULT <volatile>, PIVOT) comes back
  // as the bare body and raw_statement_count stays 1 -- we run that body as one
  // prepared unit (see SetCompound). Genuinely separate commands
  // (raw_statement_count > 1) are rejected: a prepared statement holds exactly
  // one command -- one parameter list, one result descriptor.
  duckdb::idx_t raw_statement_count = 0;
  _connection_ctx->AcquireCatalogSnapshot();
  auto extracted =
    _conn->ExtractStatements(query, &raw_statement_count, /*wrap_multi=*/false);
  if (raw_statement_count > 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("cannot insert multiple commands into a prepared statement"));
  }
  // In an aborted transaction block PG rejects every command except
  // COMMIT/ROLLBACK -- before emitting ParseComplete and without defining the
  // statement name. An empty query string runs nothing, so it stays allowed.
  if (!extracted.empty() && !_txn_state->GuardNotAborted(*extracted[0])) {
    ThrowAbortedTransaction();
  }
  // Named statements are stored only after a successful parse: a syntax error
  // must win over "already exists" and must not define the name.
  if (!statement_name.empty()) {
    slot = &_proto.statements.Create(statement_name);
  }
  Statement& statement = **slot;
  if (extracted.empty()) {
    // Empty query string is legal: Execute replies EmptyQueryResponse.
    statement.MakeEmpty();
    WriteEmptyFrame(this->_send, PQ_MSG_PARSE_COMPLETE);
    return;
  }
  const auto copy = ClassifyCopy(*extracted[0]);
  if (copy.dir == CopyDir::FromStdin || copy.dir == CopyDir::ToStdout) {
    // COPY FROM STDIN defers because the CSV sniff would open /dev/stdin before
    // the feeder is live; every COPY TO STDOUT defers so the session frames the
    // stream at Execute (the wire collector for binary/text, DuckDB's writer
    // for csv/json/parquet/...), rather than running as a plain Prepared
    // statement.
    statement.SetCopy(std::move(extracted[0]));
    WriteEmptyFrame(this->_send, PQ_MSG_PARSE_COMPLETE);
    return;
  }
  // Run + drop leading temporary-DDL scaffolding (a value-extracting PIVOT's
  // temp enums) now, so the remaining statement(s) prepare against it: a PIVOT
  // then reduces to a single prepared SELECT (Describe/Bind work as normal). A
  // non-temporary expansion (ALTER ADD COLUMN ... DEFAULT <volatile>) keeps all
  // its statements and runs as a compound at Execute.
  while (extracted.size() > 1 && IsTemporaryCreate(*extracted.front())) {
    auto result = _conn->Query(std::move(extracted.front()));
    if (result->HasError()) {
      _proto.statements.Undefine(statement_name);
      result->GetErrorObject().Throw();
    }
    extracted.erase(extracted.begin());
  }
  if (extracted.size() == 1) {
    // The temp-DDL scaffolding above ran full query lifecycles whose statement
    // end released the Parse-time snapshot.
    auto bind_snapshot = _connection_ctx->AcquireCatalogSnapshot();
    auto prepared = _conn->Prepare(std::move(extracted[0]),
                                   type_hints.empty() ? nullptr : &type_hints);
    if (prepared->HasError()) {
      // A failed prepare must not define the name (the slot is dropped below;
      // the unnamed slot stays Unbound).
      const auto error = prepared->GetErrorObject();
      _proto.statements.Undefine(statement_name);
      error.Throw();
    }
    statement.SetPrepared(std::move(prepared), std::move(bind_snapshot));
  } else {
    // One user command expanded into several statements (wrap_multi=false left
    // the body bare, no BEGIN/COMMIT). Keep them UNPREPARED: a later
    // sub-statement binds against the catalog an earlier one leaves (the
    // backfill UPDATE references the column the ADD COLUMN adds), so preparing
    // them now -- before the ADD COLUMN has run -- fails "Referenced update
    // column ... not found". They are prepared and run in sequence at Execute,
    // inside the session's implicit block (ExecuteCompound).
    statement.SetCompound(std::move(extracted));
  }
  WriteEmptyFrame(this->_send, PQ_MSG_PARSE_COMPLETE);
}

template<SocketKind Kind>
BindInfo PgWireSession<Kind>::ParseBindVars(std::string_view cursor,
                                            const Statement& stmt,
                                            std::string_view statement_name) {
  const auto& prepared = stmt.GetPrepared();
  const auto input_formats = ParseBindFormats(cursor);
  if (cursor.size() < sizeof(uint16_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Bind message"));
  }
  const auto params = absl::big_endian::Load16(cursor.data());
  cursor.remove_prefix(sizeof(uint16_t));
  // PG rejects a Bind whose parameter count differs from the prepared
  // statement's at Bind time (before BindComplete), not later at Execute.
  if (const auto required = prepared.named_param_map.size();
      params != required) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("bind message supplies ", params,
                            " parameters, but prepared statement \"",
                            statement_name, "\" requires ", required));
  }
  if (input_formats.size() > 1 && input_formats.size() != params) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("bind message has ", input_formats.size(),
                            " parameter formats but ", params, " parameters"));
  }

  duckdb::vector<duckdb::Value> values;
  values.reserve(params);
  const auto snapshot = _connection_ctx->AcquireCatalogSnapshot();
  sdb::pg::DeserializeContext dctx{snapshot.get()};
  sdb::pg::FillDeserializeContext(_connection_ctx->GetClientContext(), dctx);
  for (uint16_t i = 0; i < params; ++i) {
    if (cursor.size() < sizeof(int32_t)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                      ERR_MSG("malformed Bind message"));
    }
    const auto length =
      static_cast<int32_t>(absl::big_endian::Load32(cursor.data()));
    cursor.remove_prefix(sizeof(int32_t));
    if (length == -1) {
      values.emplace_back();
      continue;
    }
    if (length < 0 || cursor.size() < static_cast<size_t>(length)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                      ERR_MSG("invalid parameter length: ", length));
    }
    const auto format = FormatFor(input_formats, i);
    const auto type = ResolveExpectedType(prepared.data->value_map, i);
    const auto field = cursor.substr(0, length);
    const auto fn =
      sdb::pg::GetDeserialization<sdb::pg::ValueSink>(type, format);
    duckdb::Value out;
    sdb::pg::ValueSink sink{type, out};
    if (!fn(dctx, field, sink)) {
      if (format == sdb::pg::VarFormat::Binary) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
          ERR_MSG("incorrect binary data format in bind parameter ", i + 1));
      }
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
        ERR_MSG("invalid input syntax for bind parameter ", i + 1));
    }
    values.emplace_back(std::move(out));
    cursor.remove_prefix(length);
  }
  auto output_formats = ParseBindFormats(cursor);
  if (const auto columns = prepared.data->types.size();
      output_formats.size() > 1 && output_formats.size() != columns) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
      ERR_MSG("bind message has ", output_formats.size(),
              " result formats but query has ", columns, " columns"));
  }
  return BindInfo{std::move(output_formats), std::move(values)};
}

template<SocketKind Kind>
void PgWireSession<Kind>::HandleBind(std::string_view payload) {
  const auto portal_end = payload.find('\0');
  const auto statement_end = portal_end == std::string_view::npos
                               ? std::string_view::npos
                               : payload.find('\0', portal_end + 1);
  if (statement_end == std::string_view::npos) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Bind message"));
  }
  const std::string_view portal_name = payload.substr(0, portal_end);
  const std::string_view statement_name =
    payload.substr(portal_end + 1, statement_end - portal_end - 1);
  payload.remove_prefix(statement_end + 1);

  auto& slot = _proto.statements.FindOrThrow(statement_name);
  auto& statement = *slot;
  if (!statement.Bound()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SQL_STATEMENT_NAME),
                    ERR_MSG("prepared statement is not ready"));
  }
  // Aborted transaction block: reject the Bind (no BindComplete) unless the
  // bound statement is COMMIT/ROLLBACK.
  const bool is_txn_exit =
    statement.GetKind() == Statement::Kind::Prepared &&
    statement.GetPrepared().data &&
    statement.GetPrepared().data->unbound_statement &&
    IsTransactionExit(*statement.GetPrepared().data->unbound_statement);
  if (_txn_state->StatusByte() == 'E' && !is_txn_exit) {
    ThrowAbortedTransaction();
  }

  Portal portal;
  portal.stmt = slot;  // co-own the plan
  // Only a Prepared statement carries parameters and result columns; a deferred
  // COPY or the empty query binds with none. Planning (PendingQuery) is
  // deferred to Execute, where max_rows picks the wire-collector (full drain)
  // vs the streaming (cursor paging) path; plan-time errors surface there.
  if (statement.GetKind() == Statement::Kind::Prepared) {
    portal.bind_info = ParseBindVars(payload, statement, statement_name);
  }

  _proto.portals.Create(portal_name) = std::move(portal);
  WriteEmptyFrame(this->_send, PQ_MSG_BIND_COMPLETE);
}

template<SocketKind Kind>
void PgWireSession<Kind>::DescribeStatement(Statement& stmt) {
  switch (stmt.GetKind()) {
    case Statement::Kind::Unbound:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SQL_STATEMENT_NAME),
                      ERR_MSG("prepared statement is not ready"));
    case Statement::Kind::Empty:
    case Statement::Kind::Copy:
    case Statement::Kind::Compound:
      // No parameters, no result columns. (A Compound is an all-DDL/DML
      // expansion -- its last sub-statement returns no rows.)
      WriteParameterDescription(this->_send, {});
      WriteEmptyFrame(this->_send, PQ_MSG_NO_DATA);
      return;
    case Statement::Kind::Prepared:
      break;
  }
  auto& prepared = stmt.GetPrepared();
  const auto return_type = prepared.GetStatementProperties().return_type;
  // A data-returning Describe can't run in an aborted block (RowDescription
  // would touch the catalog); PG rejects it BEFORE sending anything -- no
  // ParameterDescription. A non-data statement stays describable.
  if (return_type == duckdb::StatementReturnType::QUERY_RESULT &&
      _txn_state->StatusByte() == 'E') {
    ThrowAbortedTransaction();
  }
  const auto param_count = prepared.named_param_map.size();
  std::vector<int32_t> oids;
  oids.reserve(param_count);
  for (uint16_t i = 0; i < param_count; ++i) {
    oids.push_back(
      sdb::pg::Type2Oid(ResolveExpectedType(prepared.data->value_map, i)));
  }
  WriteParameterDescription(this->_send, oids);

  if (return_type != duckdb::StatementReturnType::QUERY_RESULT) {
    WriteEmptyFrame(this->_send, PQ_MSG_NO_DATA);
    return;
  }
  WriteResolvedRowDescription(prepared, nullptr, {});
}

template<SocketKind Kind>
void PgWireSession<Kind>::DescribePortal(Portal& portal) {
  // The empty query and a deferred COPY return no rows; only a Prepared portal
  // has a RowDescription. (HandleDescribe already rejected an unbound portal.)
  if (portal.stmt->GetKind() != Statement::Kind::Prepared) {
    WriteEmptyFrame(this->_send, PQ_MSG_NO_DATA);
    return;
  }
  auto& prepared = portal.stmt->GetPrepared();
  const auto return_type = prepared.GetStatementProperties().return_type;
  if (return_type != duckdb::StatementReturnType::QUERY_RESULT) {
    WriteEmptyFrame(this->_send, PQ_MSG_NO_DATA);
    return;
  }
  if (_txn_state->StatusByte() == 'E') {
    ThrowAbortedTransaction();
  }
  WriteResolvedRowDescription(prepared, &portal.bind_info.param_values,
                              portal.bind_info.output_formats);
}

// A parameterized template may carry unresolved output types (duckdb defers
// full binding until the parameter types are known); probe-plan to describe
// the real columns, falling back to the template. `params` are the portal's
// bound values, or null for a statement-level Describe -- the probe then
// synthesizes type-derived placeholders.
template<SocketKind Kind>
void PgWireSession<Kind>::WriteResolvedRowDescription(
  duckdb::PreparedStatement& prepared, duckdb::vector<duckdb::Value>* params,
  std::span<const sdb::pg::VarFormat> formats) {
  const auto unresolved = [](const duckdb::LogicalType& type) {
    return type.id() == duckdb::LogicalTypeId::UNKNOWN ||
           type.id() == duckdb::LogicalTypeId::INVALID;
  };
  if (!prepared.named_param_map.empty() &&
      std::ranges::any_of(prepared.GetTypes(), unresolved)) {
    duckdb::vector<duckdb::Value> placeholders;
    if (!params) {
      const auto param_count = prepared.named_param_map.size();
      placeholders.reserve(param_count);
      for (size_t i = 0; i < param_count; ++i) {
        auto type = ResolveExpectedType(prepared.data->value_map, i);
        duckdb::Value value{"1"};
        if (!value.DefaultTryCastAs(type)) {
          value = duckdb::Value{type};
        }
        placeholders.emplace_back(std::move(value));
      }
      params = &placeholders;
    }
    ClosingPending pending = PendingQueryEnsured(prepared, *params, nullptr);
    if (!pending->HasError()) {
      WriteRowDescription(this->_send, pending->types,
                          duckdb::StringsToIdentifiers(pending->names),
                          formats);
      return;
    }
  }
  WriteRowDescription(this->_send, prepared.GetTypes(), prepared.GetNames(),
                      formats);
}

template<SocketKind Kind>
void PgWireSession<Kind>::HandleDescribe(std::string_view payload) {
  const auto name_end =
    payload.size() < 2 ? std::string_view::npos : payload.find('\0', 1);
  if (name_end == std::string_view::npos || name_end + 1 != payload.size()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Describe message"));
  }
  const char what = payload.front();
  const std::string_view name = payload.substr(1, name_end - 1);
  if (what == 'S') {
    DescribeStatement(*_proto.statements.FindOrThrow(name));
  } else if (what == 'P') {
    // Find (not FindOrThrow): the anon slot always exists but an unbound anon
    // portal (no stmt) is "does not exist" too, so both misses share one throw.
    Portal* portal = _proto.portals.Find(name);
    if (portal == nullptr || portal->stmt == nullptr) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CURSOR_NAME),
                      ERR_MSG("portal \"", name, "\" does not exist"));
    }
    DescribePortal(*portal);
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("invalid Describe target"));
  }
}

template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::HandleExecute(std::string_view payload) {
  const auto name_end = payload.find('\0');
  if (name_end == std::string_view::npos ||
      payload.size() < name_end + 1 + sizeof(int32_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Execute message"));
  }
  const std::string_view portal_name = payload.substr(0, name_end);
  const auto requested_rows = static_cast<int32_t>(
    absl::big_endian::Load32(payload.data() + name_end + 1));
  // Postgres treats max_rows <= 0 as "fetch all" -> the unpaged fast path.
  const uint64_t max_rows =
    requested_rows > 0 ? static_cast<uint64_t>(requested_rows) : 0;

  auto& portal = _proto.portals.FindOrThrow(portal_name);
  if (!portal.stmt) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CURSOR_NAME),
                    ERR_MSG("portal is not bound"));
  }
  switch (portal.stmt->GetKind()) {
    case Statement::Kind::Unbound:
      // Never bound, or a deferred COPY a prior Execute already consumed.
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CURSOR_NAME),
                      ERR_MSG("portal is not bound"));
    case Statement::Kind::Empty:
      WriteEmptyFrame(this->_send, PQ_MSG_EMPTY_QUERY_RESPONSE);
      co_return {};
    case Statement::Kind::Copy:
      co_await RunDeferredCopy(portal);
      co_return {};
    case Statement::Kind::Prepared:
      co_await ExecutePrepared(portal, max_rows);
      co_return {};
    case Statement::Kind::Compound:
      // The first Execute prepares + runs the leading sub-statements in order
      // and prepares the last against the now-updated catalog
      // (ExecuteCompound); ExecutePrepared then drives that last one --
      // emitting the single CommandComplete and honoring max_rows -- and a
      // re-Execute reaches it too.
      if (portal.exec.state == PortalState::Unstarted) {
        co_await ExecuteCompound(portal);
      }
      co_await ExecutePrepared(portal, max_rows);
      co_return {};
  }
}

template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::RunDeferredCopy(Portal& portal) {
  // Consume the stashed COPY (the plan reverts to Unbound) so a stray
  // re-Execute or a second portal bound to it gets a clean "not bound" rather
  // than a re-run.
  auto deferred = portal.stmt->TakeCopy();
  const auto copy = ClassifyCopy(*deferred);
  if (copy.dir == CopyDir::ToStdout) {
    if (IsNativeCopyFormat(copy.format)) {
      co_await RunCopyToStdout(std::move(deferred), copy.format);
    } else {
      // csv/json/parquet/...: DuckDB writes the bytes, the session frames.
      co_await RunCopyToStdoutViaFormat(std::move(deferred), copy.format);
    }
  } else {
    // COPY FROM STDIN: RunCopyFromStdin brings the feeder live before the
    // sniff.
    co_await RunCopyFromStdin(std::move(deferred));
  }
  co_return {};
}

template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::ExecutePrepared(Portal& portal,
                                                    uint64_t max_rows) {
  auto& prepared = portal.stmt->GetPrepared();
  auto& exec = portal.exec;
  const auto return_type = prepared.GetStatementProperties().return_type;

  // An Execute defers its commit to the Sync: the first one after a Sync opens
  // the implicit block (postgres exec_execute_message defers all regular stmts,
  // commit fires at the Sync case) so every Execute in the pre-Sync batch runs
  // in one MetaTransaction, and the Sync drops the portals it owned -- a paged
  // cursor survives across Flush (which does not commit) but not Sync, as in
  // postgres. A leading BEGIN/COMMIT/ROLLBACK is excluded: it must reach the
  // transaction operator itself, not run wrapped in our block.
  if (prepared.GetStatementType() !=
      duckdb::StatementType::TRANSACTION_STATEMENT) {
    _txn_state->Arm();
  }

  // Plan + drive exactly once per portal (planning was deferred from Bind so
  // max_rows can pick the path); a re-Execute resumes the retained pipeline.
  if (exec.state == PortalState::Unstarted) {
    if (return_type != duckdb::StatementReturnType::QUERY_RESULT) {
      // DDL/DML/SET: materialize, write the command tag (affected-row count),
      // done in this Execute -- these never page.
      auto result = co_await DriveToResult(
        prepared, portal.bind_info.param_values, exec.pending, nullptr,
        portal.stmt->BindSnapshot());
      WriteCommandTag(prepared, *result, return_type);
      exec.state = PortalState::Exhausted;
      if (prepared.GetStatementType() ==
          duckdb::StatementType::TRANSACTION_STATEMENT) {
        // COMMIT/ROLLBACK closes the block and drops the portals it owned
        // (incl. this one); portal dangles after, hence the co_return.
        AfterTxnStatement();
      }
      co_return {};
    }
    auto wire = MakeWireContext(portal.bind_info.output_formats);
    if (max_rows == 0) {
      // Full drain: rows flow executor -> _send via the wire collector; the
      // portal completes within this Execute.
      co_await DriveToResult(prepared, portal.bind_info.param_values,
                             exec.pending, wire, portal.stmt->BindSnapshot());
      exec.state = PortalState::Exhausted;
      WriteCommandComplete(this->_send, prepared,
                           wire->rows.load(std::memory_order_relaxed));
      co_return {};
    }
    // Paged: the single Direct sink encodes into _send and suspends at the row
    // budget. The ctx + pending are retained per portal (exec) so an
    // interleaved portal cannot clobber them; a re-Execute raises the budget.
    wire->paged = true;
    wire->row_budget.store(max_rows, std::memory_order_relaxed);
    exec.wire = std::move(wire);
    exec.pending =
      PendingQueryEnsured(prepared, portal.bind_info.param_values, exec.wire);
    ThrowIfError(*exec.pending);
    exec.state = PortalState::Paging;
  } else if (exec.state == PortalState::Paging && exec.wire) {
    // Re-Execute of a paged portal: raise the budget (max_rows==0 drains all).
    exec.wire->row_budget.store(
      max_rows == 0
        ? std::numeric_limits<uint64_t>::max()
        : exec.wire->row_budget.load(std::memory_order_relaxed) + max_rows,
      std::memory_order_relaxed);
  }

  // Re-Execute of a portal already drained by a previous Execute: don't fetch
  // -- just complete with 0 rows (postgres's atEnd path).
  if (exec.state == PortalState::Exhausted) {
    WriteCommandComplete(this->_send, prepared, 0);
    co_return {};
  }

  // Paged QUERY_RESULT: drive the shared loop; the Direct sink stops it at the
  // row budget. Bytes are already in _send (writer/Flush drains them). Report
  // this Execute's own row count (postgres semantics).
  auto& wire = *exec.wire;
  const auto rows_before = wire.rows.load(std::memory_order_relaxed);
  const auto status = co_await DriveQuery(*exec.pending, &wire);
  if (wire.rows.load(std::memory_order_relaxed) >=
      wire.row_budget.load(std::memory_order_relaxed)) {
    WriteEmptyFrame(this->_send, PQ_MSG_PORTAL_SUSPENDED);
    co_return {};
  }
  // Drained before the budget -> the portal is exhausted.
  ThrowIfDriveFailed(*exec.pending, status);
  auto result = exec.pending->Execute();
  ThrowIfError(*result);
  exec.state = PortalState::Exhausted;
  WriteCommandComplete(this->_send, prepared,
                       wire.rows.load(std::memory_order_relaxed) - rows_before);
  co_return {};
}

template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::ExecuteCompound(Portal& portal) {
  // One user command the parser expanded into several statements (ALTER TABLE
  // ADD COLUMN ... DEFAULT <volatile> -> [ADD COLUMN; UPDATE backfill; SET
  // DEFAULT]). Prepare and run each leading sub-statement in turn -- NOT at
  // Parse -- because a later one binds against the catalog the previous left:
  // the backfill UPDATE references the column the ADD COLUMN just added, so
  // preparing it before the ADD ran fails "Referenced update column ... not
  // found". The body is atomic, so Arm() opens the session's implicit block
  // (commit at Sync / rollback on error ride the session, and the per-statement
  // DDL -> catalog refresh lets the backfill bind the new column); a leading
  // user BEGIN already owns the block, so Arm() is a no-op. The last
  // sub-statement is prepared here too (catalog now final) and installed for
  // ExecutePrepared, which emits the one CommandComplete.
  _txn_state->Arm();
  auto& body = portal.stmt->GetCompound();
  duckdb::vector<duckdb::Value> no_params;
  for (duckdb::idx_t i = 0; i + 1 < body.size(); ++i) {
    auto bind_snapshot = _connection_ctx->AcquireCatalogSnapshot();
    auto prepared = _conn->Prepare(std::move(body[i]));
    ThrowIfError(*prepared);
    ClosingPending pending;
    co_await DriveToResult(*prepared, no_params, pending, nullptr,
                           std::move(bind_snapshot));
  }
  auto bind_snapshot = _connection_ctx->AcquireCatalogSnapshot();
  auto last = _conn->Prepare(std::move(body.back()));
  ThrowIfError(*last);
  portal.stmt->SetCompoundResult(std::move(last), std::move(bind_snapshot));
  co_return {};
}

template<SocketKind Kind>
void PgWireSession<Kind>::HandleClose(std::string_view payload) {
  const auto name_end =
    payload.size() < 2 ? std::string_view::npos : payload.find('\0', 1);
  if (name_end == std::string_view::npos || name_end + 1 != payload.size()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Close message"));
  }
  const char what = payload.front();
  const std::string_view name = payload.substr(1, name_end - 1);
  if (what == 'P') {
    _proto.portals.Drop(name);
  } else if (what == 'S') {
    // Closing a prepared statement no longer cascades into bound portals: each
    // portal co-owns its plan via the StatementPtr, so it survives until its
    // own Close or the transaction boundary (PG DEALLOCATE semantics).
    _proto.statements.Drop(name);
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("invalid Close target"));
  }
  WriteEmptyFrame(this->_send, PQ_MSG_CLOSE_COMPLETE);
}

template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::Run() {
  auto self = this->shared_from_this();
  if (_active != nullptr) {
    _active->fetch_add(1, std::memory_order_relaxed);
  }
  metrics::Add(metrics::Gauge::PgConnections);
  absl::Cleanup conn_guard = [this] {
    if (_active != nullptr) {
      _active->fetch_sub(1, std::memory_order_relaxed);
    }
    metrics::Sub(metrics::Gauge::PgConnections);
  };
  // Detach-then-Unregister on every exit path, while _conn is still alive, so
  // a racing Cancel/Terminate can never touch a torn-down context or session.
  absl::Cleanup cancel_guard = [this] {
    _cancel_token->Detach();
    if (_cancel && _cancel_key) {
      _cancel->Unregister(_cancel_key);
    }
  };
  // The writer drains committed bytes (incl. startup/auth replies) on a raw
  // `this`, concurrently with everything below; joined before this frame ends.
  auto writer = this->SendWriter();
  if (co_await Negotiate()) {
    // Bring-up (catalog snapshot, CreateConnection, GUC application) is heavy
    // and runs once on the duck worker, not here; hand the recv/send roles to
    // the duck-side half and degrade into a byte pump.
    auto cpu = SpawnSession();
    try {
      co_await PumpRecv();
    } catch (const std::exception&) {
      // Teardown boundary, not error handling: every socket op is NoThrow, so a
      // throw here is a genuine escape -- fall through to stop + join.
    }
    this->Stop();
    co_await std::move(cpu);
  }
  this->Stop();
  co_await std::move(writer);
  co_return {};
}

template<SocketKind Kind>
yaclib::Task<bool> PgWireSession<Kind>::Negotiate() {
  // Slowloris guard: TLS handshake + startup + auth must finish within
  // --auth_timeout, else close. Cancelled on every exit. The handler holds a
  // self, so a fire racing teardown is harmless.
  absl::Cleanup deadline_guard = [this] { _deadline.cancel(); };
  if (_auth_timeout.count() > 0) {
    _deadline.expires_after(_auth_timeout);
    _deadline.async_wait(
      [self = this->shared_from_this()](const asio_ns::error_code& ec) {
        if (!ec) {
          self->_socket.Close();
        }
      });
  }
  if (!co_await this->ReadProxyPreface(_proxy)) {
    co_return false;
  }
  // TODO: ssl handshake can be here, but there's no always ssl in pg-wire
  try {
    StartupRequest startup;
    if (co_await NegotiateStartup(startup) == StartupOutcome::Close) {
      co_return false;
    }
    _params = std::move(startup.params);

    // PG validates the `replication` value during startup: a value that is
    // neither a bool nor "database" is FATAL, not a silent normal connection.
    if (startup.replication_invalid) {
      const auto it = _params.find("replication");
      WriteFatalResponse(
        this->_send,
        SQL_ERROR_DATA(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("invalid value for parameter \"replication\": \"",
                  it != _params.end() ? std::string_view{it->second}
                                      : std::string_view{},
                  "\""),
          ERR_HINT(
            "Valid values are: \"false\", 0, \"true\", 1, \"database\".")));
      co_await this->Flush();
      co_return false;
    }

    // SereneDB has no WAL/walsender, so refuse replication connections with a
    // clear error instead of mishandling the replication sub-protocol.
    if (startup.replication) {
      WriteFatalResponse(
        this->_send,
        SQL_ERROR_DATA(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("replication connections are not supported by SereneDB")));
      co_await this->Flush();
      co_return false;
    }

    // PostgreSQL requires a non-empty user name in the startup packet; the role
    // itself is verified later (once RBAC lands) via the CredentialProvider
    // seam.
    const auto user = _params.find("user");
    if (user == _params.end() || user->second.empty()) {
      WriteFatalResponse(
        this->_send,
        SQL_ERROR_DATA(
          ERR_CODE(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
          ERR_MSG("no PostgreSQL user name specified in startup packet")));
      co_await this->Flush();
      co_return false;
    }
    // PostgreSQL uses the user name as the database when none is given.
    if (const auto [it, inserted] =
          _params.try_emplace("database", user->second);
        !inserted && it->second.empty()) {
      it->second = user->second;
    }

    if (_max_conn != 0 && _active != nullptr &&
        _active->load(std::memory_order_relaxed) > _max_conn) {
      WriteFatalResponse(this->_send,
                         SQL_ERROR_DATA(ERR_CODE(ERRCODE_TOO_MANY_CONNECTIONS),
                                        ERR_MSG("too many connections")));
      co_await this->Flush();
      co_return false;
    }

    // Authenticate before establishing the connection / sending
    // AuthenticationOk (which SendStartupBurst does). TLS is already settled
    // (the SSLRequest upgrade ran in NegotiateStartup), so IsTls() is final.
    if (!co_await Authenticate()) {
      co_await this->Flush();
      co_return false;
    }
  } catch (const std::exception&) {
    co_return false;
  }
  co_return true;
}

template<SocketKind Kind>
auto PgWireSession<Kind>::NegotiateStartup(StartupRequest& startup)
  -> yaclib::Task<StartupOutcome> {
  // PG accepts at most one SSL and one GSS negotiation request; a duplicate of
  // either is a protocol error that terminates the connection.
  bool ssl_done = false;
  bool gss_done = false;
  for (;;) {
    const auto frame =
      co_await NextFrame(FrameKind::Startup, MAX_STARTUP_PACKET_LENGTH);
    if (frame.status != FrameStatus::Ok) {
      co_return StartupOutcome::Close;
    }
    const uint32_t code = StartupCode(frame.payload);
    if (code == NEGOTIATE_SSL_CODE) {
      _frames.Consume(frame);
      if (ssl_done) {
        WriteFatalResponse(
          this->_send,
          SQL_ERROR_DATA(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                         ERR_MSG("received duplicate SSLRequest")));
        co_await this->Flush();
        co_return StartupOutcome::Close;
      }
      ssl_done = true;
      if constexpr (Kind == SocketKind::MaybeTls) {
        if (!this->_socket.IsTls()) {
          // CVE-2021-23214: the client must send nothing between SSLRequest and
          // the TLS ClientHello -- any buffered plaintext here is a bug or a
          // MITM injection, so refuse rather than process it post-TLS.
          if (this->_recv.Readable()) {
            co_return StartupOutcome::Close;
          }
          {
            message::Writer writer{this->_send};
            writer.Write("S");
            writer.Commit(false);
          }
          co_await this->Flush();
          if (co_await this->_socket.Handshake().NoThrow()) {
            co_return StartupOutcome::Close;
          }
          this->_socket.MarkTls();
          // A completed TLS handshake also rules out a later GSS request.
          gss_done = true;
          continue;
        }
      }
      {
        message::Writer writer{this->_send};
        writer.Write("N");
        writer.Commit(false);
      }
      co_await this->Flush();
      continue;
    }
    if (code == NEGOTIATE_GSS_CODE) {
      _frames.Consume(frame);
      if (gss_done) {
        WriteFatalResponse(
          this->_send,
          SQL_ERROR_DATA(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                         ERR_MSG("received duplicate GSSENCRequest")));
        co_await this->Flush();
        co_return StartupOutcome::Close;
      }
      gss_done = true;
      {
        message::Writer writer{this->_send};
        writer.Write("N");
        writer.Commit(false);
      }
      co_await this->Flush();
      continue;
    }
    if (code == static_cast<uint32_t>(CANCEL_REQUEST_CODE)) {
      // CancelRequest payload: code(4) + backend pid(4) + secret(4). Look the
      // (pid,secret) up in the shared registry and interrupt the target's
      // running query cross-thread. No reply -- just close (per protocol).
      if (_cancel && frame.payload.size() >= 12) {
        const uint32_t pid = absl::big_endian::Load32(frame.payload.data() + 4);
        const uint32_t secret =
          absl::big_endian::Load32(frame.payload.data() + 8);
        _cancel->Cancel((uint64_t{pid} << 32) | secret);
      }
      co_return StartupOutcome::Close;
    }
    startup = ParseStartup(frame.payload);
    _frames.Consume(frame);
    if (_require_tls && !this->_socket.IsTls()) {
      WriteFatalResponse(this->_send,
                         SQL_ERROR_DATA(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                                        ERR_MSG("SSL connection is required")));
      co_await this->Flush();
      co_return StartupOutcome::Close;
    }
    constexpr auto kLatestMajor =
      static_cast<uint32_t>(PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST));
    constexpr auto kLatestMinor =
      static_cast<uint32_t>(PG_PROTOCOL_MINOR(PG_PROTOCOL_LATEST));
    if (startup.major != kLatestMajor) {
      WriteFatalResponse(
        this->_send,
        SQL_ERROR_DATA(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                       ERR_MSG("unsupported frontend protocol ", startup.major,
                               ".", startup.minor, ": server supports ",
                               kLatestMajor, ".", kLatestMinor)));
      co_await this->Flush();
      co_return StartupOutcome::Close;
    }
    // A newer minor version or unrecognized _pq_ options are negotiated down to
    // what we support rather than dropping the connection.
    if (startup.minor > kLatestMinor || !startup.unrecognized_pq.empty()) {
      WriteNegotiateProtocolVersion(this->_send,
                                    static_cast<int32_t>(PG_PROTOCOL_LATEST),
                                    startup.unrecognized_pq);
    }
    co_return StartupOutcome::Proceed;
  }
}

template<SocketKind Kind>
yaclib::Future<> PgWireSession<Kind>::SpawnSession() {
  this->_task = duckdb::make_shared_ptr<CpuResumer>(
    duckdb::TaskScheduler::GetScheduler(DuckDBEngine::Instance().instance()),
    *this->_ioexec);
  // SessionMain (eager) runs to its first Park, setting the resume job; the one
  // bootstrap kick then schedules it onto a duck worker.
  auto cpu = SessionMain();
  this->_task->RequestRun();
  return cpu;
}

template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::PumpRecv() {
  for (;;) {
    auto [ec, n] =
      co_await this->_socket.ReadSome(this->_recv.Reserve(kReadBlock))
        .NoThrow();
    if (ec || n == 0) {
      co_return {};
    }
    this->_recv.CommitWrite(n);
    // During COPY FROM STDIN the io-pinned feeder is the recv consumer; the
    // SessionTask is parked in DriveQuery and does not need message wakes.
    if (_copy_route.load(std::memory_order_acquire)) {
      _copy_gate.Kick();
    } else {
      this->_task->RequestRun();
    }
  }
}

template<SocketKind Kind>
yaclib::Future<> PgWireSession<Kind>::SessionMain() {
  co_await this->_task->Park();
  // From here every resume is a fresh Execute() frame on a duck worker.
  absl::Cleanup finish_guard = [this] { this->_task->Finish(); };
  {
    if (SetupConnection()) {
      {
        absl::MutexLock lock{&_cancel_token->mu};
        _cancel_token->ctx = _conn->context.get();
      }
      SendStartupBurst();
      this->KickSend();
      co_await RunCommandLoop();
    }
  }
  // Teardown runs where everything was created: results/portals die on a duck
  // worker; their cleanup may emit notices that must be stolen before
  // ~ConnectionContext asserts an empty queue.
  _proto.Clear();
  if (_connection_ctx) {
    _connection_ctx->ConsumeNotices([](const sdb::pg::SqlErrorData&) {});
  }
  // Last responses (e.g. up to the Terminate) may still be draining; closing
  // mid-write would truncate them, so drain first, then stop -- SendWriter (io)
  // closes the socket, which unwinds the recv side. No side-channel post.
  co_await this->DrainSendOnTask();
  this->Stop();
  co_return {};
}

template<SocketKind Kind>
yaclib::Task<> PgWireSession<Kind>::RunCommandLoop() {
  // The post-error discard mode (see PostError); a coroutine makes it a plain
  // loop local. Flush/Sync and the post-error skip are handled before dispatch;
  // the switch is the only place a command runs.
  PostError discard = PostError::Resync;
  for (;;) {
    auto frame = co_await AwaitFrame(FrameKind::Typed, _max_message);
    if (frame.status == FrameStatus::TooLarge) {
      // PG logs an over-cap message server-side only (COMMERROR is
      // LOG_SERVER_ONLY) and drops the connection with no wire reply; we
      // deliberately send a FATAL first so the client learns why it was
      // dropped, then close (no desync risk -- we never read the body).
      WriteFatalResponse(
        this->_send,
        SQL_ERROR_DATA(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       ERR_MSG("incoming message exceeds maximum size")));
      co_return {};
    }
    if (frame.status != FrameStatus::Ok || frame.type == PQ_MSG_TERMINATE) {
      co_return {};
    }
    const char type = frame.type;

    // Crash injection for recovery testing: abort on the first client message
    // after `SET sdb_faults = 'crash_on_packet'`, simulating a mid-stream crash
    // so tests can exercise restart recovery.
    SDB_IF_FAILURE("crash_on_packet") { SDB_IMMEDIATE_ABORT(); }

    if (type == PQ_MSG_FLUSH) {
      _frames.Consume(frame);
      this->KickSend();
      continue;
    }
    if (type == PQ_MSG_SYNC) {
      discard = PostError::Resync;
      _frames.Consume(frame);
      CommitAndReportReady();
      continue;
    }
    if (discard == PostError::DiscardUntilSync) {
      _frames.Consume(frame);
      continue;
    }

    _dispatch_consume = frame.recv_consume;
    try {
      switch (type) {
        case PQ_MSG_QUERY:
          co_await RunSimpleQuery(frame.payload);
          break;
        case PQ_MSG_PARSE:
          HandleParse(frame.payload);
          break;
        case PQ_MSG_BIND:
          HandleBind(frame.payload);
          break;
        case PQ_MSG_DESCRIBE:
          HandleDescribe(frame.payload);
          break;
        case PQ_MSG_EXECUTE:
          co_await HandleExecute(frame.payload);
          break;
        case PQ_MSG_CLOSE:
          HandleClose(frame.payload);
          break;
        case PQ_MSG_COPY_DATA:
        case PQ_MSG_COPY_DONE:
        case PQ_MSG_COPY_FAIL:
          // Accept but ignore COPY frames received outside a COPY: a COPY that
          // errored leaves the client still streaming data for a moment. PG
          // discards these rather than raising a protocol error.
          break;
        case PQ_MSG_FUNCTION_CALL:
          // The legacy fast-path function-call sub-protocol (libpq large-object
          // calls). We don't implement it; reject gracefully and keep the
          // connection -- as PG does on a fast-path error -- instead of the
          // FATAL close an unknown message type gets. Self-syncs below like a
          // simple Query (it carries no following Sync).
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG("fast-path function calls are not supported"));
        default:
          // An unrecognized frontend message type is a fatal protocol error in
          // PG -- the stream is desynced, so terminate rather than try to
          // resync at the next Sync.
          WriteFatalResponse(
            this->_send,
            SQL_ERROR_DATA(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                           ERR_MSG("invalid frontend message type ",
                                   static_cast<int>(type))));
          co_await this->Flush();
          co_return {};
      }
    } catch (const std::exception& exception) {
      discard = FunnelError(exception, type);
    }
    DrainNotices();
    // GUC ParameterStatus is reported only at ReadyForQuery (in
    // CommitAndReportReady), as PG does -- not after each dispatched message --
    // so a SET LOCAL reverted by the block commit nets to no spurious report.
    if (_dispatch_consume) {
      this->_recv.Consume(_dispatch_consume);
      _dispatch_consume = 0;
    }

    // Simple Query (and the standalone fast-path FunctionCall) is self-syncing:
    // it resolves its own implicit block and emits ReadyForQuery here. Extended
    // replies instead accumulate and go out at the client's Sync (handled
    // above). Neither waits for the socket -- the next frame is consumed while
    // the response drains.
    if (type == PQ_MSG_QUERY || type == PQ_MSG_FUNCTION_CALL) {
      CommitAndReportReady();
    }
  }
}

template class PgWireSession<SocketKind::MaybeTls>;
template class PgWireSession<SocketKind::Tcp>;
template class PgWireSession<SocketKind::Unix>;

}  // namespace sdb::network::pg
